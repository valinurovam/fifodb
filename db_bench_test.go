package fifodb

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func BenchmarkDBPut16(b *testing.B) {
	benchmarkDBPut(16, b)
}

func BenchmarkDBPut32(b *testing.B) {
	benchmarkDBPut(32, b)
}

func BenchmarkDBPut64(b *testing.B) {
	benchmarkDBPut(64, b)
}

func BenchmarkDBPut128(b *testing.B) {
	benchmarkDBPut(128, b)
}

func BenchmarkDBPut256(b *testing.B) {
	benchmarkDBPut(256, b)
}

func BenchmarkDBPut1024(b *testing.B) {
	benchmarkDBPut(1024, b)
}

func BenchmarkDBPut4096(b *testing.B) {
	benchmarkDBPut(4096, b)
}

func BenchmarkDBPut16384(b *testing.B) {
	benchmarkDBPut(16384, b)
}

func BenchmarkDBPut65536(b *testing.B) {
	benchmarkDBPut(65536, b)
}

func BenchmarkDBGet16(b *testing.B) {
	// benchmarkDBGet(16, b)
}
func BenchmarkDBGet64(b *testing.B) {
	// benchmarkDBGet(64, b)
}
func BenchmarkDBGet256(b *testing.B) {
	// benchmarkDBGet(256, b)
}
func BenchmarkDBGet1024(b *testing.B) {
	// benchmarkDBGet(1024, b)
}
func BenchmarkDBGet4096(b *testing.B) {
	// benchmarkDBGet(4096, b)
}
func BenchmarkDBGet16384(b *testing.B) {
	// benchmarkDBGet(16384, b)
}
func BenchmarkDBGet65536(b *testing.B) {
	// benchmarkDBGet(65536, b)
}

//func BenchmarkDBPutMult16(b *testing.B) {
//	benchmarkDBPutMulti(16, b)
//}
//func BenchmarkDBPutMult64(b *testing.B) {
//	benchmarkDBPutMulti(64, b)
//}

func benchmarkDBPut(size int64, b *testing.B) {
	b.SetBytes(size)
	opt := DefaultOptions
	opt.Path = "test/bench_db" + strconv.Itoa(int(size)) + "_" + strconv.Itoa(b.N) + "_" + strconv.Itoa(int(time.Now().Unix()))

	db, _, err := Open(opt)

	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := db.Push(data)
		if err != nil {
			b.Fatal("Unexpected error on benchmark", err)
		}
	}
	b.StopTimer()

	//err = db.Close()
	//if err != nil {
	//	b.Fatal(err)
	//}

	err = os.RemoveAll(db.GetPath())
	if err != nil {
		b.Fatal(err)
	}
}

func benchmarkDBGet(size int64, b *testing.B) {
	b.StopTimer()
	dqName := "test/bench_disk_queue_put_" + strconv.Itoa(int(size)) + "_" + strconv.Itoa(b.N) + "_" + strconv.Itoa(int(time.Now().Unix()))
	opt := DefaultOptions
	opt.Path = dqName
	st, _, _ := Open(opt)
	b.SetBytes(size)
	data := make([]byte, size)
	defer st.Close()

	for i := 0; i < b.N; i++ {
		_, _, err := st.Push(data)
		if err != nil {
			panic(err)
		}
	}
	st.Sync()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, err := st.Pop()
		if err != nil {
			panic(i)
		}
	}
}
