package goose

import (
	"github.com/lib/pq"
	toml "github.com/pelletier/go-toml"

	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// DBDriver encapsulates the info needed to work with
// a specific database driver
type DBDriver struct {
	Name    string
	OpenStr string
	Import  string
	Dialect SqlDialect
}

type DBConf struct {
	MigrationsDir string
	Env           string
	Driver        DBDriver
}

// extract configuration details from the given file
func NewDBConf(p, env string) (*DBConf, error) {

	cfgFile := filepath.Join(p, "config", env+".toml")

	config, err := toml.LoadFile(cfgFile)
	if err != nil {
		return nil, err
	}

	drv, ok := config.Get("db.driver").(string)
	if ! ok {
		return nil, err
	}

	open, ok := config.Get("db.dsn").(string)
	if ! ok {
		return nil, err
	}
	open = os.ExpandEnv(open)

	// Automatically parse postgres urls
	if drv == "postgres" {

		// Assumption: If we can parse the URL, we should
		if parsedURL, err := pq.ParseURL(open); err == nil && parsedURL != "" {
			open = parsedURL
		}
	}

	d := newDBDriver(drv, open)

	// allow the configuration to override the Import for this driver
	if imprt, ok := config.Get("migrate.import").(string); ok {
		d.Import = imprt
	}

	// allow the configuration to override the Dialect for this driver
	if dialect, ok := config.Get("migrate.dialect").(string); ok {
		d.Dialect = dialectByName(dialect)
	}

	if !d.IsValid() {
		return nil, errors.New(fmt.Sprintf("Invalid DBConf: %v", d))
	}

	return &DBConf{
		MigrationsDir: filepath.Join(p, "migrations"),
		Env:           env,
		Driver:        d,
	}, nil
}

// Create a new DBDriver and populate driver specific
// fields for drivers that we know about.
// Further customization may be done in NewDBConf
func newDBDriver(name, open string) DBDriver {

	d := DBDriver{
		Name:    name,
		OpenStr: open,
	}

	switch name {
	case "postgres":
		d.Import = "github.com/lib/pq"
		d.Dialect = &PostgresDialect{}

	case "mymysql":
		d.Import = "github.com/ziutek/mymysql/godrv"
		d.Dialect = &MySqlDialect{}
	
	case "mysql":
		d.Import = "github.com/go-sql-driver/mysql"
		d.Dialect = &MySqlDialect{}
	}

	return d
}

// ensure we have enough info about this driver
func (drv *DBDriver) IsValid() bool {
	return len(drv.Import) > 0 && drv.Dialect != nil
}
