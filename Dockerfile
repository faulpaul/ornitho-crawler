FROM webdevops/apache:ubuntu-16.04

RUN apt-get clean
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y python3 python3-urllib3 python3-lxml python3-requests python3-bs4 locales tzdata cron python3-mysqldb

# set locale for german dates
RUN locale-gen de_DE.UTF-8
ENV LANG='de_DE.UTF-8' LANGUAGE='de_DE.UTF-8' LC_ALL='de_DE.UTF-8'
RUN ln -fs /usr/share/zoneinfo/Europe/Berlin /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

ADD ./scripts/ /scripts/

CMD ["python3", "/scripts/crawler_ornithode.py"]
