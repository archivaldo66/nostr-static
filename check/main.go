// Diagnóstico: consulta cada relay de config.yaml por TODOS los artículos
// (kind 30023) de tu cuenta y muestra qué relay tiene qué.
// Uso:  go run ./check
package main

import (
	"context"
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
	Profiles []string `yaml:"profiles"`
}

type found struct {
	d       string
	title   string
	created nostr.Timestamp
	id      string
}

type relayResult struct {
	url   string
	err   string
	items map[string]found // clave: d-tag
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

	// Autor: se toma del primer naddr válido
	pubkey := ""
	wantD := map[string]string{} // d-tag -> naddr del config
	var order []string
	for _, n := range c.Articles {
		prefix, data, err := nip19.Decode(strings.TrimSpace(n))
		if err != nil || prefix != "naddr" {
			continue
		}
		ep := data.(nostr.EntityPointer)
		if pubkey == "" {
			pubkey = ep.PublicKey
		}
		wantD[ep.Identifier] = n
		order = append(order, ep.Identifier)
	}
	if pubkey == "" {
		fmt.Println("No pude sacar tu clave pública de los naddr de config.yaml")
		os.Exit(1)
	}

	// quitar relays duplicados
	seen := map[string]bool{}
	var relays []string
	for _, r := range c.Relays {
		r = strings.TrimSpace(r)
		if r != "" && !seen[r] {
			seen[r] = true
			relays = append(relays, r)
		}
	}

	fmt.Printf("Consultando %d relays por los artículos de %s...\n\n", len(relays), pubkey[:8])

	results := make([]relayResult, len(relays))
	var wg sync.WaitGroup
	for i, url := range relays {
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			results[i] = query(url, pubkey)
		}(i, url)
	}
	wg.Wait()

	// ---- Resumen por relay
	var out strings.Builder
	w := func(format string, a ...any) {
		s := fmt.Sprintf(format, a...)
		fmt.Print(s)
		out.WriteString(s)
	}

	w("=== RESUMEN POR RELAY ===\n")
	for _, r := range results {
		if r.err != "" {
			w("  %-42s ERROR: %s\n", r.url, r.err)
		} else {
			w("  %-42s %d artículos\n", r.url, len(r.items))
		}
	}

	// ---- Todos los artículos distintos hallados
	all := map[string]found{}
	where := map[string][]string{}
	for _, r := range results {
		for d, f := range r.items {
			where[d] = append(where[d], r.url)
			if cur, ok := all[d]; !ok || f.created > cur.created {
				all[d] = f
			}
		}
	}

	w("\n=== ARTÍCULOS ENCONTRADOS EN ALGÚN RELAY: %d ===\n", len(all))
	var ds []string
	for d := range all {
		ds = append(ds, d)
	}
	sort.Slice(ds, func(a, b int) bool { return all[ds[a]].created > all[ds[b]].created })
	for _, d := range ds {
		f := all[d]
		tag := "(NO está en tu config.yaml)"
		if _, ok := wantD[d]; ok {
			tag = "(ya está en config)"
		}
		w("\n- %s  [%s]\n  d=%s %s\n  en %d relay(s): %s\n",
			f.title, time.Unix(int64(f.created), 0).Format("2006-01-02"), d, tag,
			len(where[d]), strings.Join(where[d], ", "))
	}

	// ---- Artículos del config que NINGÚN relay devolvió
	w("\n=== ARTÍCULOS DE TU config.yaml QUE NINGÚN RELAY TIENE ===\n")
	missing := 0
	for _, d := range order {
		if _, ok := all[d]; !ok {
			missing++
			w("  d=%s\n", d)
		}
	}
	if missing == 0 {
		w("  (ninguno: todos aparecen en al menos un relay)\n")
	}

	// ---- Bloque listo para pegar en config.yaml
	w("\n=== NADDR LISTOS PARA PEGAR EN config.yaml (artículos hallados) ===\narticles:\n")
	for _, d := range ds {
		f := all[d]
		naddr := nip19.EncodePointer(nostr.EntityPointer{
			PublicKey:  pubkey,
			Kind:       nostr.KindArticle,
			Identifier: d,
			Relays:     where[d],
		})
		w("  - %s # %s\n", naddr, f.title)
	}

	if err := os.WriteFile("resultado-relays.txt", []byte(out.String()), 0644); err == nil {
		fmt.Println("\nGuardado también en resultado-relays.txt")
	}
}

func query(url, pubkey string) relayResult {
	res := relayResult{url: url, items: map[string]found{}}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// 1) ¿conecta el relay?
	probe, err := nostr.RelayConnect(ctx, url)
	if err != nil {
		res.err = "no conecta (" + short(err) + ")"
		return res
	}
	probe.Close()

	// 2) pedir TODOS tus artículos (misma llamada que usa nostr-static)
	pool := nostr.NewSimplePool(ctx)
	filter := nostr.Filter{
		Kinds:   []int{nostr.KindArticle},
		Authors: []string{pubkey},
		Limit:   500,
	}
	for ev := range pool.FetchMany(ctx, []string{url}, filter) {
		d := ev.Tags.GetD()
		title := "(sin título)"
		if t := ev.Tags.GetFirst([]string{"title"}); t != nil && len(*t) > 1 {
			title = (*t)[1]
		}
		if cur, ok := res.items[d]; !ok || ev.CreatedAt > cur.created {
			res.items[d] = found{d: d, title: title, created: ev.CreatedAt, id: ev.ID}
		}
	}
	return res
}

func short(err error) string {
	s := err.Error()
	if len(s) > 70 {
		s = s[:70] + "…"
	}
	return s
}
