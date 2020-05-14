FROM jupyter/scipy-notebook
USER root

# Spark dependencies
ENV APACHE_SPARK_VERSION 2.4.4
ENV HADOOP_VERSION 2.7

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    rm -rf /var/lib/apt/lists/*

CMD [ cd /tmp]

CMD [wget -q http://mirrors.ukfast.co.uk/sites/ftp.apache.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz] 


CMD [echo"2E3A5C853B9F28C7D4525C0ADCB0D971B73AD47D5CCE138C85335B9F53A6519540D3923CB0B5CEE41E386E49AE8A409A51AB7194BA11A254E037A848D0C4A9E5 *spark-${APACHE_SPARK_VERSION}-bin-hadoop {HADOOP_VERSION}.tgz" | sha512sum -c -]

CMD [tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner] 

CMD [rm spark-${APACHE_SPARK_VERSION}-bin-hadoop{HADOOP_VERSION}.tgz]
CMD [cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark]

# Spark config
ENV SPARK_HOME /usr/local/spark-${APACHE_SPARK_VERSION}
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip
ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info


# Set the working directory to /app

WORKDIR /app


RUN conda install -c conda -y pyspark

# Installing the package for generating maps
RUN conda install -y basemap 

RUN cd /tmp && \
    git clone https://github.com/matplotlib/basemap.git && \
    cp -f ./basemap/lib/mpl_toolkits/basemap/data/epsg ../opt/conda/share/proj && \
    rm -rf basemap && \
    cd ..

VOLUME /app

# Make port available to the world outside this container
EXPOSE 8888

# ENTRYPOINT allows us to specify the default executible
ENTRYPOINT ["python", ‫"‬myTweapp.py"]