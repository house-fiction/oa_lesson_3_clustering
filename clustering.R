#R Code for K-Means example
#install.packages("scatterplot3d")
#install.packages("RColorBrewer")
require(scatterplot3d)
require(RColorBrewer)

#ORE specific functions
ore.connect("MOVIEDEMO", "orcl", "localhost", "welcome1")
ore.sync()
ore.ls()
ore.attach()

#If the dataset is sitting locally
#cluster_set <- read.table("cluster_set.dat", header=FALSE, sep="\t")
#names(cluster_set) <- c("x","y","z")

#Pull into main memory because ORE doesn't handle classes as expected
cluster_set <- ore.pull(CUSTOMER_CLUSTERING)
cluster_set <- cluster_set[complete.cases(cluster_set),]
jpeg("pre_clustering.jpeg")
par(mfrow=c(3,1))
plot(cluster_set$X, cluster_set$Y)
plot(cluster_set$Y, cluster_set$Z)
plot(cluster_set$X, cluster_set$Z)
dev.off()
cl <- kmeans(cluster_set[,c("X","Y","Z")],3,nstart=10)
cluster.pal <- brewer.pal(length(cl$centers[,1]),"Set1")
jpeg("post_clustering.jpg")
par(mfrow=c(2,2))
plot(cluster_set$X, cluster_set$Y, col=cluster.pal[cl$cluster])
plot(cluster_set$Y, cluster_set$Z, col=cluster.pal[cl$cluster])
plot(cluster_set$X, cluster_set$Z, col=cluster.pal[cl$cluster])
scatterplot3d(cluster_set[,c("X","Y","Z")], color=cluster.pal[cl$cluster], angle=100)
dev.off()
