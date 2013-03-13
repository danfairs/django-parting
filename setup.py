from distutils.core import setup

VERSION = '0.0.1'

setup(
    name='parting',
    version=VERSION,
    author='Dan Fairs',
    author_email='dan@fezconsulting.com',
    license='BSD',
    description='Tools to help manage large database tables',
    install_requires=[
        'Django',
        'South'
    ]
)
