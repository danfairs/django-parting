from setuptools import setup, find_packages

VERSION = '0.0.2'

setup(
    name='django-parting',
    version=VERSION,
    author='Dan Fairs',
    author_email='dan@fezconsulting.com',
    license='BSD',
    description='Tools to help manage large database tables',
    install_requires=[
        'Django',
        'python-dateutil',
        'django-dfk',
    ],
    keywords='django partition database table',
    classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Django",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License"
    ],
    packages=find_packages(exclude=['ez_setup']),
    url='https://github.com/danfairs/django-parting',
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
)
