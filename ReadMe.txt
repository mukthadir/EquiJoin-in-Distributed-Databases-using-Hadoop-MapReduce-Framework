This is the command that I have used.

sudo -u mukthadir /home/mukthadir/Downloads/hadoop-2.6.2/bin/hadoop jar ~/workspace/equijoin/target/equijoin-0.0.1-SNAPSHOT.jar mapreduce/equijoin/EquiJoin hello.txt output

Mapper - My mapper is taking the key as the JoinColumn as resuffling the inputs as follws:

Suppose the key is 1.
Let the tuples are
(R, 1, 32, name)
(Suresh, 1, 433, name)
(Hello, 1, 32, something)
(foo, 3, bar, baz, bazz)

After mapping, for key 1, results will be
key = 1
value = ((R, 1, 32, name),(Suresh, 1, 433, name),(Hello, 1, 32, something))


Reducer - Reducer runs through the values obtained and saves them in an array.
Array is traversed in O(n2) but n would be significantly small here because of reshuffing nad hence
performance won't be reduced.
Join is performed if tuple[0] have different value and algorithm is based on combination theory and
not permutation, hence duplicates are eliminated.

Driver - Driver is responsible for making the job conference. Driver is written with 
the help of following tutorial:

http://www.tutorialspoint.com/hadoop/hadoop_mapreduce.htm

