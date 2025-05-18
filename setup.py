from setuptools import setup, find_packages

setup(
    name="car_market_analyzer",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "selenium>=4.11.2",
        "webdriver-manager>=4.0.0",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.2",
        "lxml>=4.9.3",
        "SQLite-utils>=3.35",
        "numpy>=1.24.3",
        "pandas>=2.0.3",
        "matplotlib>=3.7.2",
        "seaborn>=0.12.2",
        "Pillow>=10.0.0",
        "psutil>=5.9.5",
    ],
    extras_require={
        "windows": ["pywin32>=306"],
    },
    entry_points={
        "console_scripts": [
            "car_market_analyzer=src.main:main",
        ],
    },
    author="coder895",
    author_email="fearlakedesign@gmail.com",
    description="A resource-efficient car market trend analyzer for Windows",
    keywords="car, market, analysis, windows, resource-efficient",
    url="https://github.com/coder895/car_market_analyzer",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Office/Business :: Financial :: Investment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: Microsoft :: Windows",
    ],
    python_requires=">=3.8",
)
