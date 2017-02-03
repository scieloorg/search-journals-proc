FROM centos

MAINTAINER tecnologia@scielo.org

RUN yum -y install gcc
RUN yum -y install epel-release
RUN yum -y install python-devel
RUN yum -y install python-pip
RUN yum -y upgrade python-setuptools
RUN yum -y install git
RUN yum -y install libxml2 libxml2-devel libxslt-devel

COPY . /app

RUN pip install --upgrade pip
RUN chmod -R 755 /app/*

WORKDIR /app

RUN python setup.py install

CMD ["update_search", "--help"]
