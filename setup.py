from distutils.core import setup

with open('LICENSE') as handle:
    license = handle.read()

with open('README.md') as handle:
    long_description = handle.read()

setup(
    name='fafalytics',
    version='0.1dev',
    packages=['fafalytics',],
    license=license,
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points = {
        'console_scripts': ['fafalytics=fafalytics:main'],
    }
)
