// Respaldo: descarga de tus relays tus artículos (kind 30023), tu perfil (kind 0)
// y tu lista de relays (kind 10002) y los guarda como eventos originales firmados,
// uno por línea (formato .jsonl), en un archivo con la fecha de hoy.
// Uso:  go run ./backup
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"gopkg.in/yaml.v3"
)

type cfg struct {
	Relays   []string `yaml:"relays"`
	Articles []string `yaml:"articles"`
}

type key struct {
	kind int
	d    string
}

func main() {
	raw, err := os.ReadFile("config.yaml")
	if err != nil {
		fmt.Println("No encuentro config.yaml. Ejecuta esto desde la carpeta nostr-static:", err)
		os.Exit(1)
	}
	var c cfg
	if err := yaml.Unmarshal(raw, &c); err != nil {
		fmt.Println("Error leyendo config.yaml:", err)
		os.Exit(1)
	}

	pubkey := ""
	for _, n := range c.Articles {
		prefix, data, err := nip19.Decode(strings.TrimSpace(n))
		if err == nil && prefix == "naddr" {
			pubkey = data.(nostr.EntityPointer).PublicKey
			break
		}
	}
	if pubkey == "" {
		fmt.Println("No pude sacar tu clave pública de los naddr de config.yaml")
		os.Exit(1)
	}

	seen := map[string]bool{}
	var relays []string
	for _, r := range c.Relays {
		r = strings.TrimSuffix(strings.TrimSpace(r), "/")
		if r != "" && !seen[r] {
			seen[r] = true
			relays = append(relays, r)
		}
	}

	fmt.Printf("Descargando tus eventos de %d relays (puede tardar hasta 30 segundos)...\n", len(relays))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := nostr.NewSimplePool(ctx)

	latest := map[key]*nostr.Event{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, url := range relays {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			filter := nostr.Filter{
				Kinds:   []int{0, 10002, nostr.KindArticle},
				Authors: []string{pubkey},
				Limit:   500,
			}
			for ev := range pool.FetchMany(ctx, []string{url}, filter) {
				k := key{ev.Kind, ev.Tags.GetD()}
				mu.Lock()
				if cur, ok := latest[k]; !ok || ev.CreatedAt > cur.CreatedAt {
					latest[k] = ev.Event
				}
				mu.Unlock()
			}
		}(url)
	}
	wg.Wait()

	var events []*nostr.Event
	for _, ev := range latest {
		events = append(events, ev)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Kind != events[j].Kind {
			return events[i].Kind < events[j].Kind
		}
		return events[i].CreatedAt > events[j].CreatedAt
	})

	if len(events) == 0 {
		fmt.Println("No se descargó ningún evento. No se creó ningún archivo.")
		os.Exit(1)
	}

	name := "respaldo-eventos-" + time.Now().Format("2006-01-02") + ".jsonl"
	f, err := os.Create(name)
	if err != nil {
		fmt.Println("No pude crear el archivo:", err)
		os.Exit(1)
	}
	defer f.Close()

	articles, profile, relayList := 0, 0, 0
	for _, ev := range events {
		line, err := json.Marshal(ev)
		if err != nil {
			fmt.Println("No pude convertir un evento, se omite:", ev.ID)
			continue
		}
		f.Write(line)
		f.WriteString("\n")
		switch ev.Kind {
		case 0:
			profile++
		case 10002:
			relayList++
		default:
			articles++
			title := "(sin título)"
			if t := ev.Tags.GetFirst([]string{"title"}); t != nil && len(*t) > 1 {
				title = (*t)[1]
			}
			fmt.Printf("  - %s  [%s]\n", title, time.Unix(int64(ev.CreatedAt), 0).Format("2006-01-02"))
		}
	}

	fmt.Printf("\nListo. Guardado en %s\n  artículos: %d | perfil: %d | lista de relays: %d\n", name, articles, profile, relayList)
	fmt.Println("Copia ese archivo a otro lugar (un USB, tu nube) para tener un respaldo fuera de este computador.")
}
