package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/BurntSushi/toml"
)

type Metas struct {
	Metas []Meta `toml:"Metas"`
}

type Meta struct {
	Project    string        `toml:"Project"`
	Dataset    string        `toml:"Dataset"`
	Role       bq.AccessRole `toml:"Role"`
	Entity     string        `toml:"Entity"`
	EntityType bq.EntityType `toml:"EntityType"`
}

// Load reads cacheFile.
func (ms *Metas) Load(cacheFile string) error {
	if strings.HasSuffix(cacheFile, ".json") {
		f, err := os.Open(cacheFile)
		if err != nil {
			return fmt.Errorf("Failed to load metadata cache file: %v", err)
		}
		defer f.Close()

		if err := json.NewDecoder(f).Decode(ms); err != nil {
			return fmt.Errorf("Failed to load medadata cache file: %v\n  (use `bqiam cache` to create or update bigquery datasts' metadata)", err)
		}

		return nil
	}

	if strings.HasSuffix(cacheFile, ".toml") {
		if _, err := toml.DecodeFile(cacheFile, ms); err != nil {
			return fmt.Errorf("Failed to load medadata cache file: %v\n  (use `bqiam cache` to create or update bigquery datasts' metadata)", err)
		}

		return nil
	}

	return fmt.Errorf("Failed to load medadata cache file. err: unsupported file format: %s", cacheFile)
}

// Save stores the cache data to the file
func (ms *Metas) Save(cacheFile string) error {
	f, err := os.Create(cacheFile)
	if err != nil {
		return fmt.Errorf("Failed to save metadata to the file. err: %s", err)
	}
	defer f.Close()

	if strings.HasSuffix(cacheFile, ".json") {
		return json.NewEncoder(f).Encode(ms)
	}

	if strings.HasSuffix(cacheFile, ".toml") {
		return toml.NewEncoder(f).Encode(ms)
	}

	return fmt.Errorf("Failed to save metadata to the file. err: unsupported file format: %s", cacheFile)
}
