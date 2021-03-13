
import setuptools

setuptools.setup(
    name='omnidiff',
    version='0.0.1',
    python_requires='>=3.7.3',
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': ['omnidiff=omnidiff.command_line:main'],
    },
    install_requires=[
        'click>=7.1.2',
        'pytest',
        'tqdm',
    ],
    extras_require={
        'dev': [
            # https://github.com/pytest-dev/pytest/issues/7632
            'coverage>=5.2.1',
            'flake8',
            'isort',
            'mypy>=0.782',
            'pytest-cov',
            'Pygments>=2.6.1',
            'sphinx>=3.3.0',
            'wheel',
        ],
    },
    package_data={
        'omnidiff': ['py.typed'],
    },
)
