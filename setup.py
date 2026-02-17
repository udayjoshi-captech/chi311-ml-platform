from setuptools import setup, find_packages

setup(
    name="chi311-intelligence-platform",
    version="1.0.0",
    description="Chicago 311 Service Request Intelligence Platform",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">3.11",
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
    ],
)