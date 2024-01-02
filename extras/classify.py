import logging
import logging.config
import re
import shutil
from collections.abc import Generator
from pathlib import Path

import timm
import torch
from PIL import Image
from torch.nn import Module
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm

from idx_maps import in1k_map

torch.backends.cudnn.benchmark = True

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'standard',
            'filename': 'log.log',
            'mode': 'a'
        },
        'console_warning': {
            'class': 'logging.StreamHandler',
            'level': 'WARNING',
            'formatter': 'standard'
        },
        'console_info': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'filters': ['info_only']
        }
    },
    'filters': {
        'info_only': {
            '()': lambda: lambda record: record.levelno == logging.INFO
        }
    },
    'loggers': {
        'my_logger': {
            'handlers': ['file', 'console_warning', 'console_info'],
            'level': 'DEBUG'
        }
    }
})
logger = logging.getLogger(list(logging.Logger.manager.loggerDict)[-1])


class DeviceMap:
    def __init__(self, dataloader: DataLoader, device: str = 'cuda'):
        self.dataloader = dataloader
        self.device = device

    def __iter__(self):
        for imgs, fnames in self.dataloader:
            yield imgs.to(self.device, non_blocking=True, memory_format=torch.contiguous_format), fnames

    def __len__(self):
        return len(self.dataloader)


class CustomDataset(Dataset):
    def __init__(self, path_expr: Generator, transform: callable, device: str):
        self.transform = transform
        self.filenames = list(self._get_files(path_expr))
        self.device = device

    def _get_files(self, path_expr: Generator):
        return (x for x in Path(path_expr).rglob('*') if x.is_file()) if isinstance(path_expr, (Path, str)) else path_expr

    def __len__(self):
        return len(self.filenames)

    def __getitem__(self, idx: int):
        img_name = self.filenames[idx]
        try:
            img = Image.open(img_name).convert('RGB')
            img = self.transform(img) if self.transform else img
            return img, str(img_name)
        except Exception as e:
            logger.error(f'Failed to load {img_name}: {e}')
            return torch.zeros(3, *self._get_tfm_size(), device=self.device), ''

    def _get_tfm_size(self):
        for x in self.transform.transforms:
            if hasattr(x, 'size') and isinstance(x.size, (tuple, list)):
                return x.size
        return 224, 224  # default size if not found in transforms


def get_dataloader(path_expr: Generator, transform: Module, device: str, **kwargs):
    ds = CustomDataset(path_expr, transform, device)
    dl = DataLoader(ds, **kwargs)
    return DeviceMap(dl, device)


def get_timm_model(model_name: str, device: str, pretrained: bool = True):
    model = timm.create_model(model_name, pretrained=pretrained).eval().to(device)
    cfg = timm.data.resolve_model_data_config(model)
    transform = timm.data.create_transform(**cfg)
    return model, transform


def run(model_name: str, path_in: str, path_out: str, **kwargs):
    thresh = kwargs.pop('thresh', 0.9)
    topk = kwargs.pop('topk', 5)
    exclude = kwargs.pop('exclude', None)
    restrict = kwargs.pop('restrict', None)
    naming_style = kwargs.pop('naming_style', 'name')
    debug = kwargs.pop('debug', 0)
    dataloader_options = kwargs.pop('dataloader_options', {})
    device = kwargs.pop('device', 'cuda')

    path_in, path_out = map(Path, (path_in, path_out))
    if not path_out.exists():
        path_out.mkdir(parents=True, exist_ok=True)
    labels = [v[1] for k, v in in1k_map.items()]
    max_label_len = max(map(len, labels))

    model, transform = get_timm_model(model_name, device)
    dataloader = get_dataloader(path_in, transform, device, **dataloader_options)

    if naming_style == 'index':
        map_idx = -1
    elif naming_style == 'synset':
        map_idx = 0
    elif naming_style == 'name':
        map_idx = 1
    else:
        map_idx = 1

    with torch.no_grad():
        for i, (batch, fnames) in tqdm(enumerate(dataloader), desc='eval', total=len(dataloader)):
            try:
                outputs = model(batch)
                fnames = list(map(Path, fnames))
                seen = set()  # prevent dup copies to path_out
                for j, (probs, idxs) in enumerate(zip(*torch.topk(outputs.softmax(dim=1), k=topk))):
                    path = fnames[j]
                    for k in range(topk):
                        idx = idxs[k].item()
                        prob = probs[k].item()
                        label = labels[idx]

                        if debug >= 1: logger.info(f'\t{prob:<6.2f} {label:<{max_label_len}} ({path.name})')
                        if exclude and exclude(label): continue
                        if restrict and not restrict(label): continue

                        if prob >= thresh and (path not in seen):
                            if map_idx >= 0:
                                _dir = path_out / in1k_map[idx][map_idx]
                            else:
                                _dir = path_out / str(idx)
                            if not _dir.exists():
                                _dir.mkdir(parents=True, exist_ok=True)

                            seen.add(path)
                            shutil.copy2(path, _dir / path.name)  # copy with metadata
                    if debug >= 1: logger.info('')  # blank line for readability

            except Exception as e:
                logger.debug(f'Failed to classify batch: {fnames = }\t{e}')


if __name__ == '__main__':
    run(
        model_name='vit_base_patch16_clip_384.laion2b_ft_in12k_in1k',
        path_in='images',
        path_out='images_labeled',
        thresh=0.0,  # threshold for predictions, 0.9 means you want very confident predictions only
        topk=5,  # window of predictions to check if using exclude or restrict, if set to 1, only the top prediction will be checked
        exclude=lambda x: re.search('boat|ocean', x, flags=re.I),  # function to exclude classification of these predicted labels
        restrict=lambda x: re.search('sandbar|beach_wagon', x, flags=re.I),  # function to restrict classification to only these predicted labels
        dataloader_options={
            'batch_size': 32,  # *** adjust this ***
            'shuffle': False,
            'num_workers': 4,  # *** adjust this ***
            'pin_memory': True,
        },
        accumulate=False,  # accumulate results in path_out, if False, everything in path_out will be deleted before running again
        device='cuda',
        naming_style='name',  # use human-readable label names, optionally use the label index or synset
        debug=0,
    )
