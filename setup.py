import setuptools

setuptools.setup(
    name='ConvoAnalytics',
    author='Derrick Anderson - Kin & Carta',
    author_email='derrick.anderson@kinandcarta.com',
    version='0.1',
    install_requires=[
        'google-cloud-storage',
        'google-cloud-bigquery',
        'dataclasses-json'
    ],
    packages=setuptools.find_packages(),
    url='None'
)

