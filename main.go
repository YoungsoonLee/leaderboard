package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	client = &redisCluterClient{}
	ctx    = context.Background()
)

//RedisClusterClient struct
type redisCluterClient struct {
	c *redis.ClusterClient
}

// not working
func getClusterConnectionAuto() *redis.ClusterClient {

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          []string{"redis-ec-1:6379", "redis-ec-2:6379", "redis-ec-3:6379"},
		ReadOnly:       true,
		RouteByLatency: true,
		RouteRandomly:  false,
		ReadTimeout:    -1,
		MaxRetries:     10,
	})

	return rdb
}

func getClusterConnectionManual() *redis.ClusterClient {
	// clusterSlots returns cluster slots information.
	// It can use service like ZooKeeper to maintain configuration information
	// and Cluster.ReloadState to manually trigger state reloading.
	clusterSlots := func(ctx context.Context) ([]redis.ClusterSlot, error) {
		slots := []redis.ClusterSlot{
			// First node with 1 master and 1 slave.
			{
				Start: 0,
				End:   5461,
				Nodes: []redis.ClusterNode{{
					Addr: "127.0.0.1:7000", // master
				}},
			},
			// Second node with 1 master and 1 slave.
			{
				Start: 5462,
				End:   10922,
				Nodes: []redis.ClusterNode{{
					Addr: "127.0.0.1:7001", // master
				}},
			},
			// Third node with 1 master and 1 slave.
			{
				Start: 10923,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					Addr: "127.0.0.1:7002", // master
				}},
			},
		}
		return slots, nil
	}

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:   clusterSlots,
		ReadOnly:       true,
		RouteByLatency: false,
		RouteRandomly:  false,
		ReadTimeout:    -1,
		MaxRetries:     10,
	})

	return rdb
}

func getSingleConnection() *redis.Client {

	noclusterRdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return noclusterRdb
}

func main() {

	rdb := getClusterConnectionAuto()
	//rdb := getClusterConnectionManual()
	//rdb := getSingleConnection()

	ping, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("error ping: ", err)
		os.Exit(1)
	}
	fmt.Println("ping: ", ping)

	// get ranking data
	// keep index order in array !!!
	rankingKeys := []string{
		"db5:top_strain_monthly:",        //db5:top_strain_monthly:yyyy-mm:ranking
		"db5:top_total_quality_monthly:", //db5:top_total_quality_monthly:yyyy-mm:ranking
		//"db5:top_strain:ranking",
		"db5:top_networth:ranking",
		"db5:top_total_quality:ranking",
	}

	getRankData(rankingKeys, rdb)

	rdb.ReloadState(ctx)

	/*
		rdb := getSingleConnection()

		ping, err := rdb.Ping(ctx).Result()
		if err != nil {
			fmt.Println("error ping: ", err)
		}
		fmt.Println("ping: ", ping)

		getScanWithFileSingle(rdb, "db5:top_total_quality*", "key.txt")

		fmt.Println("Done.")
	*/

}

func getRankData(rankKeys []string, rdb *redis.ClusterClient) {
	// make monthly
	//monthKey := getMonthDate()

	//fmt.Println(monthKey)

	//key := ""
	for i, k := range rankKeys {
		//fmt.Println(i, k)
		if i == 0 {
			continue
			/*

				// make final ranking key
				finalKeys := makeFinalMonthlyKey(k, "ranking", monthKey)

				for _, fk := range finalKeys {
					fmt.Println(fk)

					//get data
					resultZset, err := rdb.ZRangeWithScores(ctx, fk, 0, -1).Result()
					if err != nil {
						fmt.Println("result zset error: ", err)
						os.Exit(1)
					}

					for i, v := range resultZset {
						fmt.Println(i, v)
						//make a file or zadd
					}

					//check zadd well done.

				}
			*/

		} else if i == 1 {
			//
			continue
		} else {
			fmt.Println("key: ", k)

			//get data
			resultZset, err := rdb.ZRangeWithScores(ctx, k, 0, -1).Result()
			if err != nil {
				fmt.Println("result zset error: ", err)
				os.Exit(1)
			}

			for i, v := range resultZset {
				fmt.Println(i, v)
				//make a file or zadd
			}
		}
	}

}

func setRankDataIntoNewReids() {
	// zadd(key, score, player_id)
}

func getMonthDate() []string {
	start, err := time.Parse("2006-1-2", "2017-4-1")
	if err != nil {
		fmt.Errorf("get start date error: %v", err)
	}

	end, err := time.Parse("2006-1-2", "2020-11-1")
	if err != nil {
		fmt.Errorf("get end date error: %v", err)
	}

	gap := 0
	gap = (int(end.Month()) - int(start.Month())) + 12*(int(end.Year())-int(start.Year()))
	//fmt.Println(gap)

	// make monthly
	monthKey := make([]string, 0)

	for i := 0; i <= gap; i++ {
		month := strconv.Itoa(int(start.Year())) + "-"

		if int(start.Month()) < 10 {
			month += "0" + strconv.Itoa(int(start.Month()))
		} else {
			month += strconv.Itoa(int(start.Month()))
		}

		//fmt.Println(month)
		monthKey = append(monthKey, month)
		start = start.AddDate(0, 1, 0)
	}
	return monthKey
}

func makeFinalMonthlyKey(key, field string, monthKey []string) []string {
	rk := ""

	result := make([]string, 0)
	for _, mk := range monthKey {
		rk = key + mk + ":" + field // field is ranking or players
		result = append(result, rk)
		rk = ""

	}
	return result
}
