package tailfile

import (
	"logagent/common"
)

var (
	ttMgr *tailTaskMgr
)

type tailTaskMgr struct {
	tailTaskMap      map[string]*tailObj
	collectEntryList []*common.CollectEntry
	newConfChan      <-chan []*common.CollectEntry
}

func Init(collectEntryList []*common.CollectEntry, confChan <-chan []*common.CollectEntry) (err error) {
	log.Debug("???")
	ttMgr = &tailTaskMgr{
		collectEntryList: collectEntryList,
		tailTaskMap:      make(map[string]*tailObj, 32),
		newConfChan:      confChan,
	}
	log.Debug("tailfile_mgr:Init")

	for _, conf := range collectEntryList {
		log.Debugf("current conf:%#v", conf)

		if ttMgr.exist(conf.Path) {
			log.Warnf("the log of path:%s is collecting already")
			continue
		}
		log.Debugf("start to create a tailObj for collect path:%s", conf.Path)
		tObj, err := NewTailObj(conf.Path, conf.Module, conf.Topic)
		if err != nil {
			log.Errorf("create tailObj for %s failed, err:%v", conf.Path, err)
			continue
		}
		go tObj.run()
		ttMgr.tailTaskMap[conf.Path] = tObj
		log.Debugf("create tailObj for %s success", conf.Path)
	}
	go ttMgr.run()
	return
}

func (t *tailTaskMgr) exist(path string) (isExist bool) {
	for k := range t.tailTaskMap {
		if k == path {
			isExist = true
			break
		}
	}
	return
}

func (t *tailTaskMgr) run() {
	log.Debug("wait conf change from newConf")
	for {
		newConfList := <-t.newConfChan
		log.Debugf("new conf is comming, newConf:%v", newConfList)

		for _, newConf := range newConfList {

			if t.exist(newConf.Path) {
				log.Debugf("the task of path:%s is already run", newConf.Path)
				continue
			}

			tObj, err := NewTailObj(newConf.Path, newConf.Module, newConf.Topic)
			if err != nil {
				log.Errorf("create tail task for path:%s failed, err:%v", newConf.Path, err)
				continue
			}
			log.Debugf("create new tail task for path:%s", newConf.Path)
			go tObj.run()
			t.tailTaskMap[newConf.Path] = tObj
		}

		for k := range t.tailTaskMap {
			isFound := false
			for _, nc := range newConfList {
				if k == nc.Path {
					isFound = true
					break
				}
			}
			if !isFound {

				t.tailTaskMap[k].cancel()
				log.Debugf("the task of path:%s is remove from tailTaskMap", k)
				delete(t.tailTaskMap, k)
			}
		}
	}
}
