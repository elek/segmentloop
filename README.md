# Segment loop

Helper to count distribution of segments in the database of [Storj](https://github.com/storj/storj) Distributed Storage Cloud

Segment loop is a standalone (!) segment loop to do various calculation on segments table which are not possible with SQL.

Usage:

```
./segmentloop  --db 'postgres://root@localhost:26257/metainfo_us1?sslmode=disable' --debug.addr 127.0.0.1:11111 --progress 100000
```

It will read all the files with `nodes` prefix in the current directory and print out the piece/segment distribution at the end.


```
Nodes from nodes-group1.txt (1687)                                                                                                                                                                                                                                                                                                                                          
owned pieces,number of segments                                                                                                                                                                
0,0                                                                                                                                                                                            
1,985135                                                                                                                                                                                       
2,3217081                                                                                                                                                                                      
3,7274178                                                                                                                                                                                      
4,12703783                                                                                                                                                                                     
5,18150280                                                                                                                                                                                     
6,21997889                                                                                                                                                                                     
7,23169028                                                                                                                                                                                     
8,21644841                                                                                                                                                                                     
9,18117683                                                                                                                                                                                     
10,13759099                                                                                                                                                                                    
11,9552798                                                                                                                                                                                     
12,6108814                                                                                                                                                                                     
13,3611731                                                                                                                                                                                     
14,1984779                                                                                                                                                                                     
15,1019429                                                                                                                                                                                     
16,489723                                                                                                                                                                                      
17,220382                                                                                                                                                                                      
18,93659                                                                                                                                                                                       
19,37495                                                                                                                                                                                       
20,14367                                                                                                                                                                                       
21,5141                                                                                                                                                                                        
22,1838                                                                                                                                                                                        
23,569                                                                                                                                                                                         
24,204                                                                                                                                                                                         
25,43                                                                                                                                                                                          
26,14                                                                                                                                                                                          
27,3                                                                                                                                                                                           
28,1
```
