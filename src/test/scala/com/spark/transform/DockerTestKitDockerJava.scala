package com.spark.transform

//import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.impl.spotify.DockerKitSpotify

trait DockerTestKitDockerJava extends DockerTestKit with DockerKitSpotify {}