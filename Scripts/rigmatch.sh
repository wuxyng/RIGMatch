cd /home/work/RIGMatch

export CLASSPATH=./bin:/home/work/experiments/jar/RoaringBitmap-0.8.21.jar:/home/work/experiments/jar/guava-19.0.jar:/home/work/experiments/jar/algs4.jar




#GM without node filtering
java -Xmx16384m main.GraHomSimMain ep_lb20.gra inst_lb20_cyc_m.qry

#GM with node filtering
java -Xmx16384m main.GraHomFltSimMain ep_lb20.gra inst_lb20_cyc_m.qry

#GM edge-edge mapping
java -Xmx16384m main.EdgeHomSimMain ep_lb20.gra inst_lb20_cyc_c.qry

#JM 
java -Xmx16384m main.GraHomBJMain ep_lb20.gra inst_lb20_cyc_m.qry

#TM 
java -Xmx16384m main.GraHomTJMain ep_lb20.gra inst_lb20_cyc_m.qry


