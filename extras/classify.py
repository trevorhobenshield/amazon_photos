import logging.config
import re
import shutil
from logging import getLogger, Logger
from pathlib import Path

import psutil
import timm
import torch
from PIL import Image
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
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'standard'
        },
    },
    'loggers': {
        'my_logger': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        }
    }
})
logger = getLogger(list(Logger.manager.loggerDict)[-1])


class DeviceMap:
    def __init__(self, dataloader: DataLoader, device: str = 'cuda'):
        self._dataloader = dataloader
        self._device = device

    def _map(self, batch: tuple[torch.Tensor, torch.Tensor, str]) -> tuple[torch.Tensor, torch.Tensor, str]:
        imgs, idxs, fnames = batch
        return (
            imgs.to(device=self._device),  # , memory_format=torch.channels_last, non_blocking=True),
            idxs.to(device=self._device),  # , non_blocking=True),
            fnames
        )

    def __iter__(self):
        return map(self._map, self._dataloader)


class CustomDataset(Dataset):
    def __init__(self, img_dir: Path, transform=None):
        self.image_folder = img_dir
        self.transform = transform
        self.filenames = sorted([x for x in img_dir.rglob('*') if x.is_file()])

    def __len__(self):
        return len(self.filenames)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, int, str]:
        img_name = self.image_folder / self.filenames[idx]
        img = Image.open(img_name)
        if self.transform:
            img = self.transform(img)
        return img, idx, str(self.filenames[idx])


def run(model_name: str,
        path_in: str,
        path_out: str,
        thresh: float = 0.0,
        device: str = 'cuda',
        accumulate: bool = False,
        naming_style: str = 'name',
        exclude: callable = None,
        restrict: callable = None,
        topk: int = 1,
        debug: int = 0,
        **kwargs):
    path_in, path_out = map(Path, (path_in, path_out))
    if not accumulate and path_out.exists():
        shutil.rmtree(path_out)
    path_out.mkdir(parents=True, exist_ok=True)
    labels = [v[1] for k, v in in1k_map.items()]
    max_label_len = max(map(len, labels))
    model = timm.create_model(model_name=model_name, pretrained=True).eval().to(device)

    cfg = timm.data.resolve_model_data_config(model)
    transform = timm.data.create_transform(**cfg)
    dataset = CustomDataset(Path(path_in), transform=transform)
    dataloader = DataLoader(dataset, **kwargs.pop('dataloader_options', {}))

    if naming_style == 'index':
        map_idx = -1
    elif naming_style == 'synset':
        map_idx = 0
    elif naming_style == 'name':
        map_idx = 1
    else:
        map_idx = 1

    dl = DeviceMap(dataloader, device=device)
    gen = enumerate(dl) if debug else tqdm(enumerate(dl), desc='eval', total=len(dataloader))

    with torch.no_grad():
        for i, (inputs, idxs, files) in gen:
            try:
                outputs = model(inputs)
                files = list(map(Path, files))
                for j, (prob, idx) in enumerate(zip(*torch.topk(outputs.softmax(dim=1), k=topk))):
                    for k in range(topk):
                        if debug >= 1: logger.info(f'\t{prob[k].item():<6.2f} {labels[idx[k].item()]:<{max_label_len}} ({files[j].name})')
                        if exclude and exclude(labels[idx[k].item()]): continue
                        if restrict and not restrict(labels[idx[k].item()]): continue
                        if prob[k].item() >= thresh:
                            if map_idx >= 0:
                                _dir = path_out / in1k_map[idx[k].item()][map_idx]
                            else:
                                _dir = path_out / str(idx[k].item())
                            if not _dir.exists():
                                _dir.mkdir(parents=True, exist_ok=True)
                            shutil.copy(files[j], _dir / files[j].name)
            except Exception as e:
                logger.debug(f'Failed to classify batch: {files = }\t{e}')


if __name__ == '__main__':
    run(
        'eva02_base_patch14_448.mim_in22k_ft_in22k_in1k',
        path_in='images',
        path_out='labeled',
        thresh=0.0,  # threshold for predictions, 0.9 means you only want very confident predictions
        topk=5,  # window of predictions to check if using exclude or restrict, if set to 1, only the top prediction will be checked
        exclude=lambda x: re.search('boat|ocean', x, flags=re.I),  # function to exclude classification of these predicted labels
        restrict=lambda x: re.search('sand|beach|sunset', x, flags=re.I),  # function to restrict classification to only these predicted labels
        dataloader_options={
            'batch_size': 4,  # *** adjust this ***
            'shuffle': False,
            'num_workers': psutil.cpu_count(logical=False),  # *** adjust this ***
            'pin_memory': True,
        },
        accumulate=False,  # accumulate results in path_out, if False, everything in path_out will be deleted before running again
        device='cuda',
        naming_style='name',  # use human-readable label names, optionally use the label index or synset
        debug=0,
    )
