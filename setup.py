from setuptools import setup, find_packages
import os


def parse_requirements(filename):
    result = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in (line.strip() for line in f):
            if not line or line.startswith("#"):
                continue

            if line.startswith("-r"):
                _, filename = line.split(" ", 1)
                result.extend(parse_requirements(filename))
            else:
                result.append(line)

    return result



with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

def packages():

    root, app = "base_agent", "app"
    app_packages = find_packages(root, exclude=["*tests*"])
    paths = [os.path.join(root, p.replace(".", os.sep)) for p in app_packages]
    packages = [p.replace(app, root) for p in app_packages]

    return {
        "package_dir": {p1: p2 for p1, p2 in zip(packages, paths)},
        "packages": packages,
    }

setup(
    name="base-agent",
    version="0.1.0",
    author="Pavel Knyazev",
    author_email="poulix.nova@mail.ru",
    url="https://github.com/Bondifuzz/base-agent",
    description="Entrypoint, common interface and helper utilities for each agent",
    install_requires=parse_requirements("requirements-prod.txt"),
    long_description_content_type="text/markdown",
    long_description=long_description,
    python_requires=">=3.7",
    **packages(),
)
