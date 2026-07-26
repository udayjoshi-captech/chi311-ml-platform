from setuptools import setup, find_packages

setup(
    name="chi311-intelligence-platform",
    version="1.0.0",
    description="Chicago 311 Service Request Intelligence Platform",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        # Pin cmdstanpy: prophet 1.1.6's bundled Stan binary is rejected by
        # cmdstanpy 1.3.0's stricter path validation; 1.2.4 loads it cleanly.
        "cmdstanpy==1.2.4",
        "prophet>=1.1.6",
        "mlflow>=2.9.0",
        "great-expectations>=0.18.0",
        "databricks-sql-connector>=3.0.0",
        "delta-spark>=3.0.0",
        "pyspark>=3.5.0",
    ],
)