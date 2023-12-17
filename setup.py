from textwrap import dedent

from setuptools import find_packages, setup
from pathlib import Path

install_requires = [
    'psutil',
    'nest-asyncio',
    'tqdm',
    'uvloop; platform_system != "Windows"',
    'aiofiles',
    'orjson',
    'httpx[http2]',
    'pandas',
]

about = {}
exec((Path().cwd() / 'amazon_photos' / '__version__.py').read_text(), about)

setup(
    name=about['__title__'],
    version=about['__version__'],
    author=about['__author__'],
    description=about['__description__'],
    license=about['__license__'],
    long_description=dedent((Path().cwd() / 'README.md').read_text()),
    python_requires=">=3.10.10",
    long_description_content_type='text/markdown',
    author_email='trevorhobenshield@gmail.com',
    url='https://github.com/trevorhobenshield/amazon_photos',
    install_requires=install_requires,
    keywords='amazon photos api async search automation',
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
