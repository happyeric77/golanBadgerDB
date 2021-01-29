package main

import (
	"log"
	badger "github.com/dgraph-io/badger/v2"
	"fmt"
)

func main() {

	db, err := CreateOpenDB()
	Handle(err)
	defer db.Close()

	// err = UpdateDB(db, "POSISTION", "SALES")
	// Handle(err)


	key := "NAME"
	val, err := GetValueByKey(db, key)
	Handle(err)
	fmt.Printf("value of key '%s': %s\n", key, val)

	keys, err := GetAllKey(db)
	Handle(err)
	fmt.Println(keys)

	for _, key := range keys {
		val, err := GetValueByKey(db, key)
		Handle(err)
		fmt.Printf("value of key '%s': %s\n", key, val)
	}
	
	prefix := "N"
	keysOfPrefix, err := GetKeysByPrefix(db, prefix)
	Handle(err)
	fmt.Printf("The keys which begin with %s: %s\n",prefix, keysOfPrefix)
}

// Handle error
func Handle(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// CreateOpenDB creates a new badgerDB database and open it
func CreateOpenDB() (*badger.DB, error) {
	opts := badger.DefaultOptions("./tmp/badger")
	db, err := badger.Open(opts)
	return db, err
}

// UpdateDB uses badgerDB's Update() and txn.Set() function to write key and value into database
func UpdateDB(db *badger.DB, key, value string) error {
	err := db.Update(func (txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})
	return err
}

// GetValueByKey uses badgerDB's View() and txn.Get() function to check the value of certain key in the database
func GetValueByKey (db *badger.DB, key string) (string, error) {
	var itemVal string
	err := db.View(func (txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		Handle(err)
		err = item.Value(func (val []byte) error{
			itemVal = fmt.Sprintf("%s", val)
			return nil
		})
		return err
	})
	return  itemVal, err
}

// GetAllKey iterate through all keys in database and returns a slice of key []string
func GetAllKey(db *badger.DB) ([]string, error) {
	var keys []string
	err := db.View(func(txn *badger.Txn) error{
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next(){
			itm := it.Item()
			keys = append(keys, fmt.Sprintf("%s", itm.Key()))
		}
		return nil
	})
	return keys, err
}

// GetKeysByPrefix retrieves the key with certian prefix
func GetKeysByPrefix(db *badger.DB, prefix string) ([]string, error) {
	var keys []string
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			itm := it.Item()
			keys = append(keys, fmt.Sprintf("%s", itm.Key()))
		}
		return nil
	})
	return keys, err
}

// Badger DB reference: https://iter01.com/110531.html