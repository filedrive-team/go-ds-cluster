package mongods

import (
	"reflect"
	"testing"
)

func TestExtendConf(t *testing.T) {
	dcfg := DefaultConf()
	excfg := ExtendConf(&Config{})
	if !reflect.DeepEqual(dcfg, excfg) {
		t.Fatal("should be deep equal")
	}
	uri1 := "mongodb://localhost:27010"
	excfg = ExtendConf(&Config{
		Uri: uri1,
	})
	if reflect.DeepEqual(dcfg, excfg) {
		t.Fatal("should not be deep equal")
	}
	dcfg.Uri = uri1
	if !reflect.DeepEqual(dcfg, excfg) {
		t.Fatal("should be deep equal")
	}
	dbName := "mongo_datastore"
	ref_coll_name := "ds_block_ref"
	excfg = ExtendConf(&Config{
		Uri:         uri1,
		DBName:      dbName,
		RefCollName: ref_coll_name,
	})
	if reflect.DeepEqual(dcfg, excfg) {
		t.Fatal("should not be deep equal")
	}
	dcfg = DefaultConf()
	dcfg.Uri = uri1
	dcfg.DBName = dbName
	dcfg.RefCollName = ref_coll_name
	if !reflect.DeepEqual(dcfg, excfg) {
		t.Fatal("should be deep equal")
	}
}
