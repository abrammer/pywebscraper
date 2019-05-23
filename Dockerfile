FROM continuumio/miniconda3:latest
LABEL maintainer=alan.brammer@colostate.edu

ADD environment.yml /tmp/environment.yml 
RUN conda env update -n base -f /tmp/environment.yml 
