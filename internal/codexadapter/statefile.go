package codexadapter

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// savedThread is one thread in the state file. It lets a restarted adapter keep the links of threads that
// the app-server no longer has loaded.
type savedThread struct {
	Name         string    `json:"name,omitempty"`
	Cwd          string    `json:"cwd,omitempty"`
	LastActivity time.Time `json:"lastActivity"`
}

type stateFile struct {
	Threads map[string]savedThread `json:"threads"`
}

// loadState reads the state file. A missing file is empty. A corrupt file is empty too, and the error says so
// once; the next save writes it new.
func loadState(path string) (map[string]savedThread, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: the operator gives the state file path.
	if errors.Is(err, os.ErrNotExist) {
		return map[string]savedThread{}, nil
	}
	if err != nil {
		return map[string]savedThread{}, err
	}
	var file stateFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return map[string]savedThread{}, errors.New("corrupt state file " + path + ": " + err.Error())
	}
	if file.Threads == nil {
		file.Threads = map[string]savedThread{}
	}
	return file.Threads, nil
}

// saveState writes the threads with activity after cutoff to a temp file with mode 0600 and renames it.
func saveState(path string, threads map[string]savedThread, cutoff time.Time) error {
	kept := map[string]savedThread{}
	for id, saved := range threads {
		if saved.LastActivity.After(cutoff) {
			kept[id] = saved
		}
	}
	raw, err := json.Marshal(stateFile{Threads: kept})
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	_, werr := temp.Write(raw)
	cerr := temp.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(temp.Name())
		return errors.Join(werr, cerr)
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	return nil
}
