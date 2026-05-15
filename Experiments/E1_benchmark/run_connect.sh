

# Connect
for i in $(seq 21 42)
do
    python3 ../main.py benchmark BerryFinder connect4 --columns $i > results/BerryFinder_connect4_$i.txt &
    # python3 ../main.py benchmark IDSD connect4 --columns $i --num_subgroups 100 > results/IDSD_connect4_$i.txt &
    # python3 ../main.py benchmark BSD connect4 --columns $i --num_subgroups 100 > results/BSD_connect4_$i.txt &
    # If the number of subgroups is too high, SDMapStar will take a long time to run, so we will skip it
    # if [ $i -le 33 ]
    # then
    #     python3 ../main.py benchmark SDMapStar connect4 --columns $i --num_subgroups 100 > results/SDMapStar_connect4_$i.txt &
    # fi
done
wait
