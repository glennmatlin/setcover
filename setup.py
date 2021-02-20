from setuptools import find_packages, setup

setup(
    name="setcover",
    entry_points={
        'console_scripts': [
            'setcover_run = run:main',
        ],
    },
    version="0.4.3",
    packages=find_packages(),
    description="setcover",
    author="Glenn Matlin",
    author_email="glenn.matlin@gmail.com",
)
