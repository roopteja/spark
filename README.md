# Spark

These code was used as part of my research where we propose a framework ITSKG (Imagery-based Trafficc Sensing Knowledge Graph) which utilizes the stationary traffic camera information as sensors to understand the traffic patterns. This framework adds a new dimension to existing traffic modeling systems by incorporating dynamic image-based features as well as creating a knowledge graph to add a layer of abstraction to understand and interpret concepts like congestion to the traffic event detection system.
To accomplish this, the streaming data of traffic camera images were collected from the NYSDOT endpoint and stored in an Apache Hadoop Distributed File System (HDFS) standalone cluster. We implemented image median and subtraction calculation as services in the Spark. Once the images were loaded, a [Spark service](server.py) was called to compute median images. A median image is defined as the pixel-wise median across the set of images.
This [Spark service](server.py) also performs image subtraction (where the magnitude of the differences in the intensity values of each individual pixel is computed between two images) with the help of python packages such as NumPy and SciPy.
Once the image subtraction had been computed, the result was quantified by using Manhattan norm (the sum of the absolute values between each pixel) per pixel to measure how much the image had changed. This information along with its image identifier was then stored into HDFS.
The [Spark service](server.py) can be lanched from a [Restful API](Service.java) to get the result after substracting the median images from a set of images between the specified dates.

This work has been published at Industrial Knowledge workshop, 9th International ACM Web Science Conference, Jun 2017. 
Paper Tile: "[A Knowledge Graph Framework for Detecting Traffic Events Using Stationary Cameras](https://corescholar.libraries.wright.edu/knoesis/1133/)".
Authors: M. RoopTeja, L. Sarasi, T. Banerjee, and A. Sheth