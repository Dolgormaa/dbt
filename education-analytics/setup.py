from setuptools import find_packages, setup

setup(
    name="education_analytics",
    packages=find_packages(exclude=["education_analytics_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
