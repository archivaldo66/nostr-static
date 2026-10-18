package helpers

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

// FetchEventsByAuthor hace lo mismo que FetchEvents, pero en vez de pedir cada
// naddr por separado a todos los relays, hace UNA consulta por relay
// ("todos los artículos de este autor") y luego filtra los que están en config.yaml.
// Cada relay se consulta en paralelo, así que un relay lento o caído ya no frena a los demás.
func FetchEventsByAuthor(
	relays []string,
	naddrs []string,
) ([]nostr.Event, map[string]string, error) {
	type key struct{ pub, d string }

	wanted := map[key]string{} // (autor, d) -> naddr
	pubkeys := map[string]bool{}
	relaySet := map[string]bool{}
	var relayList []string
	addRelay := func(r string) {
		r = strings.TrimSpace(r)
		if r != "" && !relaySet[r] {
			relaySet[r] = true
			relayList = append(relayList, r)
		}
	}
	for _, r := range relays {
		addRelay(r)
	}

	for _, naddr := range naddrs {
		prefix, data, err := nip19.Decode(strings.TrimSpace(naddr))
		if err != nil || prefix != "naddr" {
			log.Printf("naddr inválido, se omite: %s", naddr)
			continue
		}
		addr := data.(nostr.EntityPointer)
		if addr.Kind != nostr.KindArticle {
			log.Printf("kind inválido (%d), se omite: %s", addr.Kind, naddr)
			continue
		}
		wanted[key{addr.PublicKey, addr.Identifier}] = naddr
		pubkeys[addr.PublicKey] = true
		for _, r := range addr.Relays {
			addRelay(r)
		}
	}

	var authors []string
	for p := range pubkeys {
		authors = append(authors, p)
	}

	log.Printf("Consultando %d relays en paralelo (un solo pedido por relay)", len(relayList))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := nostr.NewSimplePool(ctx)

	latest := map[key]*nostr.Event{}
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, url := range relayList {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			filter := nostr.Filter{
				Kinds:   []int{nostr.KindArticle},
				Authors: authors,
				Limit:   500,
			}
			count := 0
			for ev := range pool.FetchMany(ctx, []string{url}, filter) {
				k := key{ev.PubKey, ev.Tags.GetD()}
				if _, ok := wanted[k]; !ok {
					continue
				}
				count++
				mu.Lock()
				if cur, ok := latest[k]; !ok || ev.CreatedAt > cur.CreatedAt {
					latest[k] = ev.Event
				}
				mu.Unlock()
			}
			log.Printf("%s: %d artículos de tu lista", url, count)
		}(url)
	}
	wg.Wait()

	var events []nostr.Event
	eventIDToNaddr := make(map[string]string)
	for k, naddr := range wanted {
		ev, ok := latest[k]
		if !ok {
			log.Printf("NO encontrado en ningún relay: %s", naddr)
			continue
		}
		tags := make(nostr.Tags, len(ev.Tags))
		for i, tag := range ev.Tags {
			tags[i] = nostr.Tag(tag)
		}
		events = append(events, nostr.Event{
			ID:        ev.ID,
			PubKey:    ev.PubKey,
			CreatedAt: ev.CreatedAt,
			Kind:      ev.Kind,
			Tags:      tags,
			Content:   ev.Content,
			Sig:       ev.Sig,
		})
		eventIDToNaddr[ev.ID] = naddr
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].CreatedAt > events[j].CreatedAt
	})

	log.Printf("Finished fetching events. Total events received: %d", len(events))
	return events, eventIDToNaddr, nil
}
