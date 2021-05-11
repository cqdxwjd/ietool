from setuptools import setup, find_packages

setup(
    name='ietool',
    version='0.0.1',
    install_requires=['future==0.18.2', 'numpy==1.16.6', 'pandas==0.24.2', 'PyHive==0.6.4',
                      'pytz==2021.1', 'sasl==0.2.1', 'six==1.16.0', 'thrift==0.13.0', 'thrift-sasl==0.4.2'],
    description='import and export data',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
    ],
    author='wangjingdong',
    url='git@code.aliyun.com:wangjingdong/ietool.git',
    author_email='wangjingdong@yunlizhihui.com',
    license='Apache 2.0',
    packages=find_packages(),
    include_package_data=False,
    zip_safe=True,
    python_requires='>=2.7',
)
