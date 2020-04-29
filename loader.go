package schd_job

import (
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/runner-mei/cron"
)

type Loader interface {
	Info() interface{}
	Load(cr *cron.Cron, arguments map[string]interface{}) error
}

var loaders = map[string]Loader{}

type SimpleLoader interface {
	Snapshots() (map[int64]time.Time, error)
	GetVersion(cron.Job) (int64, time.Time, bool)
	Load(id int64, arguments map[string]interface{}) (cron.Schedule, cron.Job, error)
}

func RegisterLoader(name string, loader SimpleLoader) {
	loaders[name] = &DefaultLoader{
		Name:       name,
		Snapshots:  loader.Snapshots,
		GetVersion: loader.GetVersion,
		Read:       loader.Load,
		GenerateID: func(id int64) string {
			return name + "-" + strconv.FormatInt(id, 10)
		},
	}
}

type DefaultLoader struct {
	Name       string
	Snapshots  func() (map[int64]time.Time, error)
	GetVersion func(cron.Job) (int64, time.Time, bool)
	Read       func(id int64, arguments map[string]interface{}) (cron.Schedule, cron.Job, error)
	GenerateID func(id int64) string
	fails      map[int64]string
	lastErr    string
}

func (loader *DefaultLoader) Info() interface{} {
	result := map[string]interface{}{}
	if loader.fails != nil {
		for id, err := range loader.fails {
			result[loader.GenerateID(id)] = err
		}
	}

	if loader.lastErr != "" {
		result["error"] = loader.lastErr
	}

	return result
}

func (loader *DefaultLoader) Load(cr *cron.Cron, arguments map[string]interface{}) error {
	loader.lastErr = ""

	versions, e := loader.Snapshots()
	if nil != e {
		loader.lastErr = "load snapshot from db failed, " + e.Error()
		return errors.New(loader.lastErr)
	}

	if versions == nil {
		versions = map[int64]time.Time{}
	}

	for _, ent := range cr.Entries() {
		id, oldVersion, ok := loader.GetVersion(ent.Job)
		if !ok {
			continue
		}

		if newVersion, ok := versions[id]; ok {
			if newVersion.Equal(oldVersion) {
				delete(versions, id)
				continue
			}

			sch, job, err := loader.Read(id, arguments)
			if err != nil {
				log.Println("["+loader.Name+"] reload '"+strconv.FormatInt(id, 10)+"' fail,", err)

				if loader.fails == nil {
					loader.fails = map[int64]string{}
				}
				loader.fails[id] = err.Error()
				continue
			}
			idStr := loader.GenerateID(id)
			cr.Unschedule(ent.Id)
			Schedule(cr, idStr, sch, job)
			delete(versions, id)
			if loader.fails != nil {
				delete(loader.fails, id)
			}

			log.Println("[" + loader.Name + "] reload '" + strconv.FormatInt(id, 10) + "' ok")
		} else {
			log.Println("["+loader.Name+"] delete job -", id)

			cr.Unschedule(loader.GenerateID(id))

			if loader.fails != nil {
				delete(loader.fails, id)
			}
		}
	}

	for id := range versions {
		sch, job, err := loader.Read(id, arguments)
		if err != nil {
			log.Println("["+loader.Name+"]] load '"+strconv.FormatInt(id, 10)+"' fail,", err)
			if loader.fails == nil {
				loader.fails = map[int64]string{}
			}
			loader.fails[id] = err.Error()
			continue
		}
		idStr := loader.GenerateID(id)
		Schedule(cr, idStr, sch, job)

		log.Println("["+loader.Name+"]] load '"+idStr+"' successful and next time is", sch.Next(time.Now()))
	}

	return nil
}
