from setuptools import setup, find_packages

setup(
    name="colabfit-tools",
    version="0.0.2",
    author="ColabFit",
    description=(
        "A suite of tools for working with training datasets for interatomic potentials"
    ),
    license="BSD",
    keywords="machine learning interatomic potentials",
    url="http://colabfit.org",
    packages=find_packages(),
    # packages=[
    #     'colabfit.tools',
    #     'colabfit.tools.configuration_sets',
    #     'colabfit.tools.configuration',
    #     'colabfit.tools.converters',
    #     'colabfit.tools.dataset',
    #     'colabfit.tools.property_settings',
    #     'colabfit.tools.property',
    #     'colabfit.tools.transformations',
    # ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    install_requires=[
        "ase",
        "kim_property",
        "numpy",
        "tqdm",
        "markdown",
        "plotly",
        "django",
        "periodictable",
        "unidecode",
        "notebook",
    ],
    extras_require={
        "postgres": ["psycopg"],
        "vast": ["vast", "pyspark", "pyarrow", "ibis"],
    },
)
