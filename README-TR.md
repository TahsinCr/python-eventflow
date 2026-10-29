[English](https://github.com/TahsinCr/python-eventflow/blob/main/README.md) | **Türkçe**

# EventFlow

[Depo](https://github.com/TahsinCr/python-eventflow) · [Son sürüm](https://github.com/TahsinCr/python-eventflow/releases/latest) · [MIT Lisansı](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE)

EventFlow, aynı Python sürecindeki bileşenlerin sıradan Python nesneleri
üzerinden haberleşmesini sağlar. Senkron bir event bus, asyncio tabanlı bir bus,
isteğe bağlı bellek içi geçmiş, isimlendirilmiş kanallar ve aboneliklerin açık
yaşam döngüsü yönetimini sunar.

İsteğe bağlı pipeline'lar değerleri isimli aşamalar ve middleware üzerinden
dönüştürür. Async stream'ler lazy dönüşümleri sınırlı tamponlama ile birleştirir.
Scope, event zarfı ve ölçüm bileşenleri, sıradan bus kullanımını değiştirmeden
bu yapıların aynı uygulamada birlikte kullanılmasını sağlar.

Kütüphane süreç içi teslimat içindir. Event'leri kalıcı olarak saklamaz,
serileştirmez veya başka süreçlere taşımaz. Bu sorumluluklar uygulamaya ya da
kullandığı mesajlaşma altyapısına aittir.

EventFlow, [Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core)
projesinin event alt sisteminden çıkarılmış ve burada bağımsız bir kütüphane
olarak geliştirilmektedir. Paketi kurmak ve kullanmak için kaynak projeye
ihtiyaç yoktur.

## İçindekiler

- [Başlangıç](#getting-started)
  - [Kurulum](#install)
  - [Sürüm bilgisi](#version)
  - [İlk event yayını](#first-event)
  - [Bus seçimi](#choose-bus)
- [Temel kavramlar](#core-concepts)
  - [Event ve abonelikler](#subscriptions)
  - [Filtreler ve teslimat limitleri](#filtering)
  - [Hatalar ve sonuçlar](#failures)
- [Event'lerle çalışma](#working-with-events)
  - [Geçmiş ve replay](#history)
  - [Event bekleme](#wait-for-event)
  - [Periyodik yayın](#periodic)
- [Async uygulamalar](#async-applications)
- [Kanalları düzenleme](#channels)
- [Callback politikaları](#callback-policies)
- [Pipeline ve middleware](#pipelines)
- [Async pipeline ve stream](#streams)
- [Abonelik scope'ları](#scopes)
- [Event context ve ölçümler](#event-context)
- [Güvenlik ve çalışma sınırları](#security-boundaries)
- [Bileşenleri birlikte kullanma](#composition)
- [Yaşam döngüsü ve başvuru](#reference)
  - [Varsayılanlar](#defaults)
  - [Bus metotları](#bus-api)
  - [Engine metotları](#engine-api)
  - [Public tipler ve hatalar](#public-types)
- [Geliştirme ve lisans](#development)

<a id="getting-started"></a>
## Başlangıç

<a id="install"></a>
### Kurulum

Python 3.10 ve üzeri gerekir. Checkout içindeki kaynak kodu etkin Python
ortamına kurun:

~~~bash
python -m pip install .
~~~

İlk PyPI yayını için seçilen dağıtım adı `eventflow-tahsincr`, Python import
paketi ise `eventflow` olarak kalır. Paket bu adla yayımlandıktan sonra
`python -m pip install eventflow-tahsincr` komutuyla kurulabilir. Python
3.10'da, Python 3.11 ve üzerindeki yerleşik özellikleri sağlayan
`backports.strenum` ve `exceptiongroup` paketleri kurulur. Paket metadata'sı bu
bağımlılıkları yalnızca Python 3.10 için etkinleştirir.

Paket içindeki kaynak kodu tip anotasyonları içerir, ancak dağıtım PEP 561
`py.typed` işaretçisini içermez. Bu nedenle kurulu paketin tip denetleyiciler
tarafından keşfedileceği garanti edilmez.

`eventflow` ve `python-eventflow` dağıtım adları PyPI'de başka projelere
kayıtlıdır ([eventflow](https://pypi.org/project/eventflow/),
[python-eventflow](https://pypi.org/project/python-eventflow/)). Bu kütüphaneyi
kurmak için `eventflow-tahsincr` adını kullanın; Python import yolu `eventflow`
olarak kalır.

Dağıtım, ortak `eventflow` import paketine dosyalar kurar. Aynı ortamda bu
paketi sağlayan başka bir dağıtım bulunması dosyaların üzerine yazılmasına veya
çakışmasına yol açabilir. İki dağıtımı birlikte kullanmanız gerekiyorsa ayrı
sanal ortamlar kullanın.

Düzenlenebilir geliştirme için isteğe bağlı araçları kurup depo kontrollerini
proje kök dizininden çalıştırın:

~~~bash
python -m pip install -e ".[dev]"
python -m unittest discover -s tests -v
ruff check .
pyright
~~~

`dev` ek paketi Ruff ve Pyright'ı sağlar. Testler Python'ın standart
`unittest` altyapısını kullanır.

<a id="version"></a>
### Sürüm bilgisi

Kurulu paketin sürüm numarasına `eventflow.__version__` üzerinden erişebilirsiniz:

~~~python
from eventflow import __version__

print(__version__)  # 1.1
~~~

<a id="first-event"></a>
### İlk event yayını

Event, gerçekleşmiş bir durumu anlatan değerdir. Herhangi bir Python nesnesi
kullanılabilir. Anlamı ve alanları açık olan değişmez bir dataclass çoğu
uygulama için iyi bir başlangıçtır.

~~~python
from dataclasses import dataclass

from eventflow import EventBus


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


def send_confirmation(event: OrderPlaced) -> None:
    print(f"Sipariş oluşturuldu: {event.order_id}")


with EventBus[OrderPlaced]() as bus:
    with bus.subscribe(send_confirmation):
        result = bus.publish(OrderPlaced("order-42"))
        print(f"teslim={result.delivered}, hata={result.failed}")
~~~

Çıktı:

~~~text
Sipariş oluşturuldu: order-42
teslim=1, hata=0
~~~

Yayıncı `OrderPlaced` değerini oluşturup bus'a verir. Bus, `publish()`
dönmeden önce kayıtlı handler'ı çağırır. İçteki context manager bittiğinde
abonelik iptal edilir; dıştaki tamamlandığında bus kapanır. Böylece iki
kaynağın da ne zaman başlayıp biteceği kullanım noktasında görülür.

`EventBus[OrderPlaced]` generic anotasyonu type checker ve IDE'lere yardımcı
olur; çalışma anında event tipini doğrulamaz. Çalışma zamanı tipi kontrolü
gerekiyorsa filtre kullanın.

<a id="choose-bus"></a>
### Bus seçimi

| | `EventBus` | `AsyncEventBus` |
| --- | --- | --- |
| Handler | Senkron callable | Async callable |
| Yayın | Eşleşen handler'lar bitene kadar bloklar | `publish()` await edilir |
| Çalışma bağlamı | Varsayılan çağıran thread; isteğe bağlı executor | Tek bir sahip asyncio event loop'u |
| Teslimat sırası | Eşzamanlı yayınlar çakışabilir | Varsayılan sıralı; isteğe bağlı concurrent |

Senkron kodda `EventBus` kullanın. Varsayılan olarak handler,
`publish()` çağrısını yapan thread'de çalışır. İsteğe bağlı
`concurrent.futures.Executor` handler'ları executor worker'larında
çalıştırabilir; ancak yayın çağrısı handler'lar tamamlanana kadar bekler.

Farklı thread'lerden yayın yapmak güvenlidir. İki yayın aynı anda yapılırsa
handler'ları da eşzamanlı çalışabilir. Paylaşılan uygulama verisini güncelleyen
handler'lar gerekiyorsa kendi kilitlerini kullanmalıdır.

Uygulama asyncio kullanıyorsa ve handler'lar coroutine ise `AsyncEventBus`
seçin. Bus'ı ilk kullanan event loop, bus'ın sahip loop'u olur. Aynı bus'ı
farklı loop'lar arasında kullanmayın. Worker thread'den event göndermek için
`publish_threadsafe()` kullanın; ayrıntılar
[Async uygulamalar](#async-applications) bölümündedir.

<a id="core-concepts"></a>
## Temel kavramlar

Bus, aktif abonelikleri tutar ve yayımlanan event'i filtresiyle eşleşen
abonelere iletir. Abonelikler birbirinden bağımsızdır: bir handler'ı iptal
etmek diğer handler'ları veya bus'ı etkilemez.

<a id="subscriptions"></a>
### Event ve abonelikler

Event'lerin kütüphaneye ait bir temel sınıftan türemesi gerekmez. Uygulamaya
uygun değerleri tanımlayıp `publish(event)` ile yayımlayın. Event nesnelerini
küçük tutun ve handler'ların ihtiyaç duyduğu bilgileri açıklayıcı alanlarda
taşıyın.

`subscribe(callback)`, `EventBus` için `Subscription`; `AsyncEventBus` için
`AsyncSubscription` döndürür. Handler'a ihtiyaç duyduğunuz sürece bu handle'ı
saklayın. İşiniz bittiğinde `cancel()` çağırın veya ilk örnekteki gibi handle'ı
context manager ile yönetin. İptali tekrarlamak güvenlidir. Handle üzerinde
`id` ve `active` özellikleri bulunur.

<a id="filtering"></a>
### Filtreler ve teslimat limitleri

Abonelik tüm event'leri, bir veya daha fazla sınıfı ya da predicate
koşulunu karşılayan event'leri alabilir. Sınıf kontrolü ve predicate birlikte
kullanılabilir:

~~~python
from dataclasses import dataclass

from eventflow import EventBus, EventFilter


@dataclass(frozen=True)
class PaymentReceived:
    account_id: str
    amount_cents: int


large_payment = EventFilter(
    event_type=PaymentReceived,
    predicate=lambda event: event.amount_cents >= 10_000,
)


def write_audit_record(event: PaymentReceived) -> None:
    print(f"Audit {event.account_id}: {event.amount_cents}")


with EventBus[PaymentReceived]() as bus:
    with bus.subscribe(write_audit_record, event_filter=large_payment):
        bus.publish(PaymentReceived("acct-7", 2_500))
        bus.publish(PaymentReceived("acct-7", 12_500))
~~~

Handler'a yalnızca ikinci ödeme ulaşır. Sınıf filtresi birden çok sınıf içeren
tuple da olabilir. Predicate, event'i alıp boolean döndüren senkron bir
fonksiyondur; async bus'ta da senkron çalışır. Filtreleme senkron olduğu için
async predicate'ler ve awaitable döndüren fonksiyonlar kabul edilmez.

Bus metodunda `EventFilter` ile doğrudan `event_type`/`predicate` argümanları
alternatif kullanımlardır; ikisini birlikte vermeyin. İlk eşleşmeden sonra
bitecek bir abonelik için `once=True` veya `once(callback, ...)` kullanın.
Belirli sayıda eşleşen teslimat için `times=N` verin. Limitler bus'a gelen
bütün event'leri değil, filtreyle eşleşenleri sayar.

<a id="failures"></a>
### Hatalar ve sonuçlar

Varsayılan `ErrorPolicy.ISOLATE` handler hatalarını kayda alır ve seçilmiş
diğer handler'lara teslimata devam eder. Her `publish()` çağrısı
`PublishResult` döndürür:

| Alan | Anlamı |
| --- | --- |
| `matched` | Bu publish çağrısında anında teslimat için seçilen abonelik sayısı |
| `delivered` | Başarıyla tamamlanan handler sayısı |
| `failed` / `errors` | Toplanan teslimat veya action hataları |
| `successful` | Hata toplanmadıysa `True` |

Replay devam ederken abonelik için tampona alınan event'ler, filtreyle eşleşse
bile `matched` sayısına girmez.

Senkron `on_error` hook'u, handler'ı belirli bir log sistemine bağlamadan
hataları raporlamak için kullanılabilir:

~~~python
from eventflow import EventBus, EventErrorContext


def report_failure(context: EventErrorContext[str]) -> None:
    print(f"{context.event}: {type(context.error).__name__}")


def persist_order(event: str) -> None:
    raise RuntimeError("depolama kullanılamıyor")


with EventBus[str](on_error=report_failure) as bus:
    bus.subscribe(persist_order)
    result = bus.publish("order-42")
    print(result.matched, result.delivered, result.failed)
~~~

Hataları çağıranın doğrudan ele alması gerekiyorsa
`error_policy=ErrorPolicy.RAISE` seçin. Bus seçilmiş handler'lara teslimatı
denedikten sonra toplanan hatalar için `ExceptionGroup` yükseltir; o yayın
çağrısı `PublishResult` döndürmez. `bus.stats` toplam `published`,
`delivered` ve `failed` sayaçlarını verir.

Bu sayaçlar her yayın için değişmez bir eşitlik oluşturmaz. `published` callback'ler
çalışmadan önce artar; `delivered` ve `failed` işlem sonunda güncellenir. Devam
eden veya iptal edilen async yayınlarda değerler farklı kalabilir. `failed`,
yalnızca başarısız handler'ları değil; handler, filtre ve action hatası
kayıtlarını sayar.

Python 3.10 ve üzerindeki sürümlerde aynı import ile gruplandırılmış hataları
yakalamak için `ExceptionGroup` sınıfını `eventflow.compatibility` modülünden
alın. Python 3.11 ve üzerindeki sürümlerde bu ad yerleşik sınıfa işaret eder.

<a id="working-with-events"></a>
## Event'lerle çalışma

<a id="history"></a>
### Geçmiş ve replay

Geçmiş varsayılan olarak kapalıdır. `history` için pozitif kapasite vererek
son event'leri sınırlı, thread-safe `MemoryEventHistory` içinde tutun. Kapasite
dolduktan sonra her yeni event en eski kaydın yerini alır. Geçmiş bellekte
kalır; kalıcı event günlüğü değildir.

~~~python
from eventflow import EventBus


with EventBus[str](history=3) as bus:
    for status in ("created", "paid", "packed", "shipped"):
        bus.publish(status)

    print(bus.query(limit=2))
    print(bus.latest())
~~~

Çıktı:

~~~text
('packed', 'shipped')
shipped
~~~

`query()` saklanan event'leri yayın sırasıyla döndürür. Aboneliklerdeki filtre
seçeneklerini ve isteğe bağlı pozitif `limit` değerini kabul eder. Limit
verildiğinde en yeni eşleşen event'leri kronolojik sırada döndürür.
`latest()` en yeni eşleşmeyi, yoksa `None` verir. History kapalıysa `query()`
boş tuple, `latest()` `None` döndürür.
History sırası event'lerin kaydedilme sırasıdır; callback'lerin başlama veya
tamamlanma sırası değildir. Async publish hook'ları kayıt sonrasında bekleyebilir
ve ayrı yayın çağrıları örtüşebilir.

Filtresiz `limit` sorgusu yalnız istenen yakın geçmiş penceresini okur. Filtreli
sorgu predicate'i kilit dışında ve sabit bir snapshot üzerinde çalıştırır.
Limitli aramada en yeniden eskiye ilerler; eşleşme çok seyrek olup en eski kayda
yakınsa yine de snapshot'ın tamamını taraması gerekebilir.
`AsyncEventBus` bu filtreli sorguyu sahibi olan event loop üzerinde senkron
çalıştırır. Büyük bir geçmişte seyrek eşleşme, diğer task'ları geciktirebilir;
replay gecikmesi önemliyse saklanan geçmişi sınırlı tutun ve predicate'leri hızlı
çalıştırın.

Sonradan katılan bir abonenin canlı teslimattan önce yakın geçmişi alması için
`replay=N` kullanın:

~~~python
with EventBus[str](history=10) as bus:
    bus.publish("started")
    bus.publish("ready")

    with bus.subscribe(print, replay=2):
        pass
~~~

Abonelik oluşturulurken handler önce `started` ve `ready` değerlerini alır.
Replay sırasında gelen canlı event'ler o abone için tamponlanır.
Varsayılan 1000 olan `replay_buffer_limit` bu geçici kuyruğu sınırlar.
Kuyruk taşarsa ilgili abonelik iptal edilir ve taşma bus'ın hata politikasıyla
raporlanır.

Özel geçmiş deposu için `EventHistory` arayüzünün `append()`, `latest()`,
`query()`, `clear()` ve `__len__()` metotlarını uygulayın. Bus üzerindeki
`clear()` aktif abonelikleri kaldırır, geçmişi silmez. Kayıtlı geçmişi
silmek için yapılandırılmış history nesnesinde `clear()` çağırın.
Yayın sırasını korumak için EventFlow, özel history nesnesinin `append()`
metodunu bus'ın yayın kilidi tutulurken çağırır. Bu metot hızlı ve bloklamayan
olmalı; aynı bus üzerinden yeniden yayın yapmamalıdır.

<a id="wait-for-event"></a>
### Event bekleme

Çağıranın uzun süreli bir handler kaydetmek yerine tek bir event'i değer
olarak alması gerekiyorsa `wait_for()` kullanın. Abonelikle aynı filtre
seçeneklerini ve isteğe bağlı timeout değerini kabul eder. Event gelirse onu,
timeout dolarsa veya eşleşme olmadan bus kapanırsa `None` döndürür. Senkron
bekleme çağıran thread'i bloklar. Timeout sonlu ve negatif olmayan bir sayı
olmalıdır; sıfır anında zaman aşımı anlamına gelir.

Aşağıdaki async örnekte yayın task'ı oluşturulduktan sonra bekleme başlar.
`wait_for()`, publisher task çalışmadan önce geçici aboneliğini kaydeder:

~~~python
import asyncio

from eventflow import AsyncEventBus


async def main() -> None:
    async with AsyncEventBus[str]() as bus:
        publish_task = asyncio.create_task(bus.publish("ready"))
        event = await bus.wait_for(event_type=str, timeout=1.0)
        await publish_task
        print(event)


asyncio.run(main())
~~~

Çıktı `ready` olur. Senkron kodda `bus.wait_for(event_type=..., timeout=...)`
kullanılır; `timeout` verilmezse süre sınırı olmadan beklenir.
`on_timeout` yapılandırılmışsa süresi dolan filtre ve timeout bilgisini
`EventTimeoutContext` içinde alır.

<a id="periodic"></a>
### Periyodik yayın

`publish_every(event, interval, times=None, immediately=True)` aynı event'i
belirli aralıklarla yayımlar ve iptal edilebilir abonelik döndürür.
`interval` pozitif olmalıdır. `immediately=False` verilmezse ilk yayın hemen
yapılır. `times` pozitif bir sayıysa zamanlama bu kadar yayından sonra biter;
daha erken durdurmak için aboneliği iptal edin. Her bus varsayılan olarak en
fazla 64 etkin zamanlamaya izin verir.

~~~python
import time

from eventflow import EventBus


with EventBus[str]() as bus:
    bus.subscribe(print)
    schedule = bus.publish_every("heartbeat", interval=1.0, times=3)

    while schedule.active:
        time.sleep(0.05)
~~~

Handler üç heartbeat alır. Async bus aynı işlemi await edilen metotla sahip
loop üzerinde yürütür:

~~~python
import asyncio

from eventflow import AsyncEventBus


async def main() -> None:
    async with AsyncEventBus[str]() as bus:
        async def report(message: str) -> None:
            print(message)

        await bus.subscribe(report)
        schedule = await bus.publish_every("heartbeat", 0.05, times=3)
        while schedule.active:
            await asyncio.sleep(0.01)


asyncio.run(main())
~~~

Bus kapandığında zamanlamaları iptal edilir. `ErrorPolicy.RAISE` seçiliyken
periyodik yayın sırasında hata oluşursa ilgili zamanlama durur. Zamanlama her
yayının `PublishResult` değerini döndürmez; handler, filtre ve action hatalarını
görmek için bus üzerinde `on_error` hook'u yapılandırın. `ErrorPolicy.ISOLATE`
seçildiğinde zamanlama sürer ve yayın sonucu başka bir yolla tutulmaz.

<a id="async-applications"></a>
## Async uygulamalar

`AsyncEventBus` async handler ve async bus hook'ları kabul eder.
Varsayılan `DeliveryMode.SEQUENTIAL`, eşleşen handler'ları abonelik sırasıyla
tek tek await eder. `DeliveryMode.CONCURRENT` handler'ları birlikte bekler.
Handler'lar birbirinden bağımsızsa concurrent kip bekleme süresini azaltabilir;
tamamlanma sırası garanti edilmez.
Sıralı teslimat garantisi tek bir `publish()` çağrısı içindedir; ayrı yayın
çağrıları birbiriyle çakışabilir. Sequential kipte handler'ın yükselttiği
`asyncio.CancelledError`, `publish()` çağrısından dışarı çıkar ve sonraki
handler'lara teslimatı durdurur. Concurrent kipte bağımsız olarak iptal edilen
handler task'ı başarısız teslimat olarak `PublishResult`'a eklenir. `publish()`
çağıran task iptal edilirse her iki kipte de iptal çağırana yayılır.

Concurrent teslimat varsayılan olarak sınırsızdır ve mevcut davranışı korur.
`DeliveryMode.CONCURRENT` ile verilen `max_concurrency=N`, her bir publish
çağrısının callback fan-out'unu sınırlar. Bir callback tamamlandığında boşalan
yuva, sabit grubun diğer callback'lerini beklemeden sıradaki aboneye verilir.
Çakışan publish çağrılarının her biri kendi sınırına sahiptir; bu seçenek
uygulamanın oluşturduğu publish çağrılarının toplamını sınırlamaz. Tek bir
event'in fan-out'u çok sayıda task veya harici kaynak tüketebilecekse sonlu bir
değer belirleyin.

~~~python
import asyncio

from eventflow import AsyncEventBus, DeliveryMode


async def main() -> None:
    async with AsyncEventBus[int](
        delivery_mode=DeliveryMode.CONCURRENT,
        max_concurrency=8,
    ) as bus:
        async def save_metric(value: int) -> None:
            print(f"Metrik kaydedildi: {value}")

        async def notify_dashboard(value: int) -> None:
            print(f"Dashboard bilgilendirildi: {value}")

        await bus.subscribe(save_metric)
        await bus.subscribe(notify_dashboard)
        result = await bus.publish(42)
        print(f"teslim={result.delivered}")


asyncio.run(main())
~~~

`subscribe()`, `publish()`, `wait_for()`, `publish_every()`, `clear()` ve
`close()` gibi işlemler await edilir. Başka bir thread'den gönderim için
`publish_threadsafe(event)` kullanın. Bu metot await edilmez; event'i sahip
loop'a gönderir ve `concurrent.futures.Future` döndürür.

~~~python
import asyncio

from eventflow import AsyncEventBus


async def main() -> None:
    async with AsyncEventBus[str]() as bus:
        async def report(message: str) -> None:
            print(message)

        await bus.subscribe(report)  # bus'ı bu loop'a bağlar
        future = await asyncio.to_thread(
            bus.publish_threadsafe,
            "worker thread'den",
        )
        result = await asyncio.wrap_future(future)
        print(result.delivered)


asyncio.run(main())
~~~

Event loop thread'inde `future.result()` çağırmayın. Bekleme loop'u bloke
eder ve event'in teslim edilmesini engeller.

<a id="channels"></a>
## Kanalları düzenleme

Uygulamanın farklı bölümleri için ayrı event alanları gerekiyorsa
`EventEngine` kanal adına göre bus oluşturup yönetir. Kanallar ilk
kullanıldıklarında oluşturulur; engine'e verilen varsayılanlar yeni bus'lara
aktarılır:

~~~python
from eventflow import EventEngine


def update_order_view(event: str) -> None:
    print(f"Sipariş görünümü güncellendi: {event}")


with EventEngine(history=50) as engine:
    engine.subscribe("orders", update_order_view)
    result = engine.publish("orders", "order-42")
    print(result.delivered)
~~~

Kanal adlarının başındaki ve sonundaki boşluklar silinir, harfler küçültülür.
Bu nedenle `" Orders "` ve `"orders"` aynı kanala gider.
`engine.channel(name)` kanalı döndürür veya oluşturur.
`engine.add(name, bus)` özel ayarlı bus kaydeder. `remove(name)` bir kanalı
kapatıp kaldırır; `stop()` tüm kanalları kapatır. Engine ayrıca `running` ve
sıralanmış `channel_names` özelliklerini sunar. Aynı bus örneği aynı anda yalnızca
tek kanal adına kaydedilebilir.
`add()` ile verilen bus o engine'in sahipliğine geçer; aynı örneği başka bir
engine'e vermeyin. Engine'lerden birinin `remove()` veya `stop()` çağrısı bus'ı
kapatır.
`stop()` sırasında iki engine de bütün kanalları kapatmayı dener. Bazı kanallar
kapatılamazsa hatalar birlikte yükseltilir ve engine bu kanalların sahipliğini
korur; sonraki `stop()` çağrısı temizliği yeniden deneyebilir. Başarılı bir
`stop()` sonrasında engine yeniden kullanılabilir; sonraki `channel()` veya
`add()` çağrısı yeni ya da açık kanallarla çalışmayı tekrar başlatır.

`AsyncEventEngine` aynı düzeni async bus'lar için sağlar. `channel()` ve
`add()` doğrudan sonuç döndürür; yayın, abonelik, bekleme, kaldırma, start ve
stop işlemleri await edilir:

~~~python
import asyncio

from eventflow import AsyncEventEngine


async def main() -> None:
    async with AsyncEventEngine(history=50) as engine:
        async def update_order_view(event: str) -> None:
            print(f"Sipariş görünümü güncellendi: {event}")

        await engine.subscribe("orders", update_order_view)
        result = await engine.publish("orders", "order-42")
        print(result.delivered)


asyncio.run(main())
~~~

`EventEngine` history, hata politikası, executor ve hook ayarlarını oluşturduğu
bus'lara aktarır. `AsyncEventEngine` history, hata politikası, teslimat kipi,
`max_concurrency` ve async hook'ları aktarır.

<a id="callback-policies"></a>
## Callback politikaları

`CallbackSubscription`, bus API'sinden bağımsız olarak tek bir callback
çağrısına limit ve hook uygulayan düşük seviyeli bir yardımcıdır.
`EventBus.subscribe()` tarafından otomatik oluşturulmaz. Bir kimlik, iptal
fonksiyonu ve callback ile kurulur:

~~~python
from eventflow import CallbackSubscription


def handle_message(message: str) -> str:
    return message.upper()


policy = CallbackSubscription(
    identifier=1,
    cancel=lambda: None,
    callback=handle_message,
    max_calls=3,
    frequency_hz=2,
    on_success=lambda context: print(context.result),
)

print(policy.invoke("ready"))
~~~

Senkron callback için `invoke()` kullanılır. Async callback için
`asynchronous=True` verip `invoke_async()` metodunu await edin:

~~~python
import asyncio

from eventflow import CallbackSubscription


async def handle_message(message: str) -> str:
    return message.upper()


async def main() -> None:
    policy = CallbackSubscription(
        identifier=2,
        cancel=lambda: None,
        callback=handle_message,
        asynchronous=True,
        timeout=1.0,
    )
    print(await policy.invoke_async("ready"))


asyncio.run(main())
~~~

Politika `once` veya `max_calls` sınırı, `frequency_hz` ile çağrı sıklığı üst
sınırı, timeout, senkron predicate, `enable()`/`disable()` ve before,
success, error, timeout, after hook'larını destekler. Hook'lar event, result,
error ve geçen süreyi içeren `CallbackContext` alır. Devre dışı, filtrelenmiş
ve hız limiti nedeniyle atlanan event'ler çağrı limitini tüketmez. Bu
kontrolleri geçen çağrı, callback sonradan hata verse bile limitten sayılır.

`on_before()`, `on_success()`, `on_error()`, `on_timeout()` ve
`on_after()` metotlarıyla ek hook kaydedilebilir. Sarmalayıcıyı doğrudan
çağırmak alttaki callback'i çalıştırır ve politikaları atlar; politika
uygulanacaksa `invoke()` veya `invoke_async()` kullanın.

Senkron timeout gözlemseldir: Python çalışan thread'i güvenli biçimde
sonlandıramaz, bu nedenle süre aşımı callback tamamlandıktan sonra bildirilir.
Async timeout işbirliğine dayalıdır: callback task'ı iptal edilir ve
`CallbackTimeoutError` yükseltilmeden önce task'ın bitmesi beklenir. İptali
yakalayıp sürdüren callback veya tamamlanmayan bir hook, çağrıyı yapılandırılmış
bütçeden uzun tutabilir. Python bir coroutine'i zorla durduramaz. Callback'in
kendi `TimeoutError` hatası, `CallbackTimeoutError` ile aynı değildir.

<!-- integrated execution documentation -->
<a id="pipelines"></a>
## Pipeline ve middleware

`Pipeline[T]`, aynı `T` tipi üzerinde sıralı dönüşümler çalıştırır. Her stage,
önceki stage'in sonucunu alır. Son değer `run()` çağrısından döner; otomatik
olarak bus'a yayımlanmaz. Boş pipeline girdisini olduğu gibi döndürür.

~~~python
from dataclasses import dataclass, replace
from eventflow import Pipeline

@dataclass(frozen=True)
class Request:
    amount: int

pipeline = Pipeline[Request]()

@pipeline.stage("validate")
def validate(request: Request) -> Request:
    if request.amount < 0:
        raise ValueError("amount negatif olamaz")
    return request

def double(request: Request) -> Request:
    return replace(request, amount=request.amount * 2)

pipeline.add("double", double, after="validate")

@pipeline.middleware("audit")
def audit(request, next_):
    print("önce", request.amount)
    result = next_(request)
    print("sonra", result.amount)
    return result

print(pipeline.run(Request(4)))  # Request(amount=8)
~~~

`stage()` ve `middleware()` kayıt yapıp orijinal fonksiyonu döndürür. Fonksiyonun
kimliği, adı ve doğrudan çağrılabilirliği korunur. Decorator ve `add()` aynı
registry'yi kullanır; kayıt sırasında fonksiyona ek bir sarmalayıcı takılmaz.

| İşlem | Davranış |
| --- | --- |
| `add(name, callback, *, before=None, after=None)` | İsteğe bağlı sıralama koşullarıyla stage ekler. |
| `stage(name, *, before=None, after=None)` | `add` işleminin decorator karşılığıdır. |
| `remove(name)` | Stage'i ve ona yapılan sıralama referanslarını kaldırır; callback'i döndürür. |
| `replace(name, callback)` | Adı, sıralamayı ve etkinlik durumunu koruyarak callback'i değiştirir. |
| `enable(name)` / `disable(name)` | Sonraki çalışmalara katılımı değiştirir. |
| `stage_names` | Devre dışı olanlar dahil sıralı stage adlarını verir. |
| `add_middleware(...)` / `middleware(...)` | Ayrı ad alanında around middleware kaydeder. |
| `remove_middleware`, `replace_middleware`, `enable_middleware`, `disable_middleware` | Aynı registry işlemlerini middleware'e uygular. |
| `middleware_names` | Devre dışı olanlar dahil dıştan içe middleware adlarını verir. |

Adların başındaki/sonundaki boşluklar temizlenir; adlar büyük/küçük harfe
duyarlıdır, boş olamaz ve kendi registry'sinde benzersizdir. `before` ve `after`
mevcut adlara başvurur. Bilinmeyen referans `KeyError`; yinelenen ad, boş ad
ve sıralama döngüsü `ValueError` üretir. Geçersiz değişiklik mevcut planı
etkilemez. Eşit durumda kayıt sırası belirleyicidir. Devre dışı stage'lerin
sıralama koşulları korunur; stage kaldırılırsa ona yapılan referanslar kaldırılır.

Sıralama konfigürasyon değiştiğinde hazırlanır. Her `run()` sabit bir execution
snapshot'ı alır; çalışma sırasında yapılan replace/disable sonraki çalışmalara
uygulanır. Konfigürasyon thread-safe'dir, callback'ler registry kilidinin dışında
çalışır. Snapshot payload'ı kopyalamaz ve callback'in kendi mutable durumunu
eşzamanlı kullanıma karşı korumaz.

Middleware tüm pipeline'ı sarar; ilk kayıt en dış katmandır. `next_(value)`
bir kez çağrılabilir, sonucu dönüştürülebilir, `PipelineExecutionError` yakalanıp
toparlanılabilir veya `next_` çağrılmadan sonuç döndürülerek kısa devre yapılabilir.
Continuation yalnız ilgili middleware çağrısı sürerken ve aynı thread/async task
içinde geçerlidir. İkinci çağrı, sonradan çağırma ve ayrı task'tan çağırma
`RuntimeError` üretir. Mevcut bus action'ları ve callback hook'ları kendi
sözleşmeleriyle çalışmaya devam eder.
Middleware iç içe continuation kullandığından her pipeline, devre dışı olanlar
dahil en fazla 64 middleware kaydını kabul eder. Sınır kayıt sırasında
doğrulanır ve çalışma anında Python çağrı yığınının aşılmasını önler.

Stage hatası sonraki stage'leri durdurur ve `PipelineExecutionError` üretir.
`name`, `phase`, `context` ve `__cause__` alanları hatanın kaynağını gösterir.
Context, başarısız bileşene verilen değerdir. Senkron stage veya middleware'in
awaitable döndürmesi reddedilir; coroutine sonucu kapatılır. Python'da senkron
callback güvenli biçimde zorla sonlandırılamaz.

<a id="streams"></a>
## Async pipeline ve stream

`AsyncPipeline[T]` aynı registry ve decorator arayüzüne sahiptir. Callback'leri
awaitable döndürür; `await pipeline.run(value)` ilk kullanılan event loop'ta
çalışır. Senkron callback'ler otomatik olarak thread'e gönderilmez. Eşzamanlı
çalışmaların snapshot ve continuation durumları birbirinden bağımsızdır.
`CancelledError` aktif stage'e iletilir; normal hataya çevrilmez ve iş yeniden
başlatılmaz.
Stage ve middleware sonucu çözülmüş bir değer olmalıdır. Dış await sonrasında
yeniden awaitable döndürülmesi (örneğin `await next_` ifadesinde await'in
unutulması) reddedilir ve dönen coroutine kapatılır.

`AsyncStream[T]` bir `AsyncIterable[T]` kaynağını sahiplenir. Dönüşümler senkron
ve awaitable callback kabul ettiğinden iki pipeline türünün `run` metotları
doğrudan map fonksiyonu olabilir. Map, filter ve batch lazy çalışır; worker
task başlatmaz.

~~~python
import asyncio
from eventflow import AsyncPipeline, AsyncStream

async def main() -> None:
    pipeline = AsyncPipeline[int]()

    @pipeline.stage("double")
    async def double(value: int) -> int:
        return value * 2

    async def source():
        for value in range(5):
            yield value

    stream = AsyncStream(source()).map(pipeline.run).filter(
        lambda value: value % 4 == 0
    ).buffer(2).batch(2)
    async with stream:
        async for batch in stream:
            print(batch)  # (0, 4), ardından (8,)

asyncio.run(main())
~~~

| İşlem | Sonuç ve kaynak sahipliği |
| --- | --- |
| `AsyncStream(source)` | Tek loop'ta bir kez tüketilen stream oluşturur. |
| `map(callback)` | Her değeri sırayla dönüştürür; awaitable sonucu bekler. |
| `filter(predicate)` | Senkron veya awaitable predicate'in kabul ettiği değerleri geçirir. |
| `batch(size, *, partial=True)` | Tuple üretir; son eksik tuple isteğe bağlıdır. |
| `buffer(capacity)` | Bir producer ve backpressure uygulayan sınırlı kuyruk ekler. |
| `aclose()` / `async with stream` | Upstream zincirini kapatır ve sahip olunan temizliği bekler. |
| `closed` | Yeni okumaların durdurulup durdurulmadığını gösterir. |

Batch boyutu ve buffer kapasitesi pozitif tamsayı olmalıdır; bool, sıfır,
kesir ve sonsuz değer reddedilir. Tampon dolduğunda producer bekler. Bellekte
en fazla `capacity` kuyruk öğesi ve kuyruğa girmeyi bekleyen bir kaynak değeri
bulunur. Sessiz veri kaybı uygulanmaz. Kaynak hatası, daha önce kuyruğa alınan
değerlerin ardından tüketiciye iletilir; normal bitiş, hata ve iptal ayrıdır.
Kapasite baytları değil nesne sayısını ölçer; birkaç büyük payload da yüksek
bellek kullanabilir. Boyutu değişken olaylarda payload boyutunu veya uygulama
tarafındaki toplam bayt bütçesini ayrıca sınırlayın.

Stream bir kez tüketilir. İkinci iterator edinimi ve eşzamanlı okuma reddedilir.
Tüketici erken çıkabilecek veya hata verebilecekse **zincirin son stream'ini**
`async with` ile yönetin. Python'ın `async for` protokolü, tüketici gövdesindeki
`break` işlemini iterator'a bildirmez; `aclose()` bu açık sahiplik sınırını
sağlar. Henüz başlatılmamış upstream zinciri de kapatılır. Başka bir task'tan
kapatma, bekleyen okuyucuyu iptal edip kaynak temizliğini bekler. Kapatmayı
çağıran task tekrar iptal edilse de sahip olunan temizlik tamamlanır ve iptal
sonrasında yayılır. Kaynak ve kullanıcı temizliği kooperatif olmalıdır;
dönmeyen kaynak kapanışı geciktirebilir.

<a id="scopes"></a>
## Abonelik scope'ları

Scope'lar `subscribe` dönüş tipini değiştirmeden mevcut iptal handle'larını
gruplar. `add(handle)` aynı handle'ı döndürür; aynı nesnenin tekrar eklenmesi
yeni kayıt oluşturmaz. Bir scope farklı bus'lardan veya callback politikalarından
gelen handle'ları birlikte yönetebilir.

~~~python
from eventflow import EventBus, SubscriptionScope

with EventBus[int]() as bus, SubscriptionScope() as scope:
    handle = scope.add(bus.subscribe(print))
    bus.publish(42)
assert not handle.active
~~~

`SubscriptionScope.close()` senkron handle'ları kayıt sırasının tersinde iptal
eder. `AsyncSubscriptionScope.aclose()` async handle'ları aynı sırayla bekler
ve `async with` destekler. İlgili sync/async `cancel()` metodunu sunan duck-typed
handle'lar kabul edilir. Her handle denenir; hatalar mevcut Python 3.10 uyumlu
`ExceptionGroup` ile toplanır ve başarısız handle'lar tekrar denemek üzere
korunur. Handle uygulaması tekrarlı iptali güvenli kılmalıdır. Handle'ın kendi
async iptali diğer handle'lar denendikten sonra yayılır. Kapanmış scope yeni
handle kabul etmez. Handle'ın diğer `BaseException` kesintileri de bütün handle'lar
denendikten sonra aynı türle yayılır; ek hatalar exception group olarak zincirlenir.
Eşzamanlı async kapanış çağrıları aynı cleanup task'ını
bekler; çağıran task'ın iptali temizlik tamamlandıktan sonra yeniden yükselir.

Scope kayıtları iptal eder; bus'ın kendisini sahiplenmez ve başlamış callback'i
zorla durdurmaz. Bus/engine kaynaklarının da kapanması için mevcut context
manager'larını kullanın. Async scope tek event loop'a aittir; kendisini bekleyecek
reentrant kapanış açıkça reddedilir.

<a id="event-context"></a>
## Event context ve ölçümler

`publish(event)` ham nesneleri kabul etmeye devam eder. İsteğe bağlı
`EventEnvelope[T]`; `event`, üretilen `id`, `correlation_id`, `source`, timezone
bilgili UTC `timestamp` ve `metadata` alanlarını sunan frozen bir veri sınıfıdır.
Metadata kopyalanıp salt okunur mapping'e alınır; payload ve metadata içindeki
nesneler kendi mutability özelliklerini korur. Event ve metadata değerleri de
uygunsa `dataclasses.asdict()` ve pickle ile dönüştürülebilir. Payload dönüşümü
için `dataclasses.replace` kullanılabilir.

~~~python
from dataclasses import replace
from eventflow import EventEnvelope, Pipeline, PipelineStats, correlation_context

stats = PipelineStats()
pipeline = Pipeline[EventEnvelope[int]](observer=stats)
pipeline.add("increment", lambda envelope: replace(envelope, event=envelope.event + 1))
with correlation_context("request-42"):
    event = EventEnvelope(1, source="example", metadata={"attempt": 1})
    result = pipeline.run(event)
print(result.event, result.correlation_id)  # 2 request-42
print(stats.snapshot.runs.successful)     # 1
~~~

`correlation_context(id)` async fonksiyon içinde de kullanılabilen normal bir
context manager'dır. `current_correlation_id()` etkin kimliği döndürür. Zarf
önce açıkça verilen correlation ID'yi, ardından bağlı context'i, son olarak
kendi ID'sini kullanır. Context asyncio task'ları arasında izoledir ve çıkışta
eski değerine döner. Yeni thread'ler ve mevcut bus executor'ı context'i otomatik
devralmaz; gerektiğinde uygulama tarafında `contextvars.copy_context` kullanın.

İki pipeline türüne de `observer=PipelineStats()` veya senkron bir
`PipelineObserver` callable verilebilir. Tamamlanan run, stage ve middleware
için `PipelineObservation(kind, name, elapsed, status, correlation_id, error)`
üretilir. Durumlar `success`, `error`, `cancelled`; süre birimi saniyedir.
Run ve stage bildirimlerini aynı kimlikle izlemek için context'i çağıran kodda
tüm run çevresinde bağlayın. Middleware içindeki context yalnız o middleware
aktifken geçerlidir. Observer kapalıyken saat okuması ve observation nesnesi
oluşturulmaz.

`stats.snapshot`; immutable `runs`, `stages`, `middleware` özetleri döndürür.
Her `ExecutionMetrics` içinde `calls`, `successful`, `failed`, `cancelled`,
`total_seconds`, `max_seconds` bulunur. Kısa devre ile atlanan stage sayılmaz.
Run ve middleware süreleri iç stage sürelerini içerdiğinden bu süreler
toplanmamalıdır. `reset()` ölçümleri temizler. Collector payload veya traceback
nesnesi tutmaz. Metrik anahtarları stage ve middleware adlarıdır; sabit adlar
collector belleğini sınırlı tutar. Dinamik adlar `reset()` çağrılana kadar
saklanır; kullanıcı veya istek değerlerini sınırsız bileşen adı olarak
kullanmayın.

Observer senkron çalışır ve `AsyncPipeline` içinde de inline çağrılır; event
loop'un gecikmemesi için hızlı ve bloklamayan olmalıdır. Log veya telemetry I/O'su
için kısa bir observation'ı uygulama tarafındaki sınırlı kuyruğa aktarın.
Observer hatası, başarılı olacak çalışmayı `phase="observer"` ile durdurur.
Mevcut bir pipeline hatası sırasında oluşan observer hataları orijinal
`PipelineExecutionError.observer_errors` üzerinde korunur; asıl cause
değiştirilmez. Mevcut `EventBusStats` alanları ve davranışı korunur.

<a id="security-boundaries"></a>
### Güvenlik ve çalışma sınırları

EventFlow callback'leri, predicate'leri, middleware'i ve stream kaynaklarını
uygulamayla aynı süreçte ve aynı izinlerle çalıştırır. Güvenilmeyen eklentileri
sandbox etmez; böyle bir kodu ayrı işletim sistemi süreci veya container içinde,
açık izinler ve kaynak sınırlarıyla çalıştırın. Async cancellation ve callback
timeout'ları işbirliğine dayalıdır; cancellation'ı bastıran veya event loop'u
bloklayan kod için kesin süre sınırı sağlamaz.

`PipelineExecutionError.context` ve observer hataları hassas veri içeren
uygulama nesnelerine referans verebilir. Bu nesneleri olduğu gibi loglamayın;
structured logging katmanında alanları ayıklayın veya redakte edin.

<a id="composition"></a>
## Bileşenleri birlikte kullanma

Bileşenler bağımsız kullanılabilir veya normal callable/iterator arayüzleriyle
birleştirilebilir. Stage olarak `CallbackSubscription.invoke`, stream map
fonksiyonu olarak `Pipeline.run` veya `AsyncPipeline.run`, scope handle'ı olarak
iki bus türünün `subscribe()` sonucu kullanılabilir. Bus yayını otomatik olarak
pipeline başlatmaz veya event'e zarf eklemez.

Depo kökünden çalıştırılabilen iki tam örnek bulunur:

~~~bash
python -m examples.integrated
python -m examples.bus_stream
~~~

[integrated.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/integrated.py);
sync/async stage'leri, decorator'ları, callback politikalarını, korelasyonu,
ölçümleri, bounded stream'i ve async bus'ı bir araya getirir.
[bus_stream.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/bus_stream.py)
ise sınırlı bir uygulama kuyruğunu bus aboneliği ve stream kaynağı olarak kullanır.
Tek publisher task'ını sahiplenip çıkışta bekler; her event için yeni task açmaz.

<a id="reference"></a>
## Yaşam döngüsü ve başvuru

<a id="defaults"></a>
### Varsayılanlar

| Seçenek | `EventBus` | `AsyncEventBus` |
| --- | --- | --- |
| `history` | `None` | `None` |
| `error_policy` | `ErrorPolicy.ISOLATE` | `ErrorPolicy.ISOLATE` |
| `executor` | `None` | Kullanılmaz |
| `delivery_mode` | Kullanılmaz | `DeliveryMode.SEQUENTIAL` |
| `max_concurrency` | Kullanılmaz | `None` (sınırsız) |
| `replay_buffer_limit` | 1000 | 1000 |
| `max_schedules` | 64 | 64 |
| `shutdown_timeout` | 5 saniye | Kullanılmaz |
| `actions` veya doğrudan `on_*` hook'ları | None | None |

`history` pozitif bir kapasite veya `EventHistory` uygulaması alır.
`actions` senkron bus için `EventBusActions`, asenkron bus için
`AsyncEventBusActions` kabul eder. `actions` nesnesini veya tek tek
`on_before`, `on_after`, `on_error` ve `on_timeout` callback'lerini verin;
ikisini birlikte kullanmayın. `EventBus` hook'ları senkron, `AsyncEventBus`
hook'ları async olmalıdır.

İşiniz bitince `close()` çağırın; context manager bunu otomatik yapar.
Kapatılmış bus yeni yayın veya abonelik kabul etmez ve `EventBusClosedError`
yükseltir. `clear()` abonelikleri kaldırır, history'yi silmez.
Her iki bus türünde de `clear()`, bekleyen `wait_for()` çağrılarını `None` ile
tamamlar. Senkron wait, bus kapandığında da `None` döndürür.

Senkron bus periyodik worker'ların durması için `shutdown_timeout` süresince
bekler. Worker zamanında durmazsa `EventShutdownTimeoutError` yükselir.
Worker zorla durdurulmaz; bloklayan handler tamamlandıktan sonra `close()`
yeniden çağrılabilir. Async kapatma periyodik task'ları iptal eder ve
tamamlanmalarını bekler. Bus'ı kapatmak sürmekte olan `publish()` çağrılarını
bekletmez; başlamış bir callback `close()` döndükten sonra tamamlanabilir.

<a id="bus-api"></a>
### Bus metotları

| Metot | Davranış |
| --- | --- |
| `subscribe(callback, *, event_filter=None, event_type=None, predicate=None, once=False, times=None, replay=0)` | Handler kaydeder. `event_filter` veya doğrudan filtre argümanlarını kullanın. |
| `once(callback, *, event_filter=None, event_type=None, predicate=None, replay=0)` | İlk eşleşen event için kayıt yapar. |
| `publish(event)` | Event'i teslim eder ve hata politikası yükseltmiyorsa `PublishResult` döndürür. |
| `wait_for(*, event_filter=None, event_type=None, predicate=None, timeout=None)` | Eşleşen bir event'i veya `None` döndürür. |
| `publish_every(event, interval, *, times=None, immediately=True)` | İptal edilebilir periyodik zamanlama başlatır. |
| `latest(...)` / `query(..., limit=None)` | Saklanan geçmişi okur. Her iki bus'ta da senkron metotlardır. |
| `clear()` / `close()` | Abonelikleri temizler veya bus'ı kapatır. `AsyncEventBus` üzerinde await edilir. |

`AsyncEventBus` üzerinde `subscribe()`, `once()`, `publish()`,
`wait_for()`, `publish_every()`, `clear()`, `close()` ve abonelik iptali
await edilir. `publish_threadsafe(event)` başka bir thread'den await etmeden
çağrılır ve `concurrent.futures.Future` döndürür.

<a id="engine-api"></a>
### Engine metotları

`EventEngine`; `channel()`, `add()`, `remove()`, `publish()`, `subscribe()`,
`once()`, `wait_for()`, `publish_every()`, `start()` ve `stop()` metotlarını
sunar. `channel()` ve `add()` doğrudan sonuç döndürür.

Engine'in `subscribe()` metodu `event_filter`, `event_type`, `predicate`,
`once`, `times` ve `replay` seçeneklerini açıkça sunar. `once()` filtre
seçeneklerini ve `replay` parametresini kabul eder. Async engine aynı seçenekleri
asenkron handler'lar için sağlar.

`AsyncEventEngine` aynı işlemleri async bus'lar için sunar. `channel()` ve
`add()` doğrudan sonuç verir; yayın, abonelik, bekleme, kaldırma, başlatma ve
durdurma işlemleri await edilir. İki engine de context manager ile kullanılabilir;
async sürümde `async with` yazılır.

<a id="public-types"></a>
### Public tipler ve hatalar

Paketten dışa aktarılan public adlar:

| Alan | Public adlar |
| --- | --- |
| Bus'lar | `EventBus`, `AsyncEventBus`, `BaseEventBus` |
| Engine'ler | `EventEngine`, `AsyncEventEngine` |
| Abonelikler | `Subscription`, `AsyncSubscription`, `CallbackSubscription` |
| Filtre ve geçmiş | `EventFilter`, `EventHistory`, `MemoryEventHistory` |
| Sonuç ve kipler | `PublishResult`, `EventBusStats`, `ErrorPolicy`, `DeliveryMode` |
| Action ve context tipleri | `EventBusActions`, `AsyncEventBusActions`, `EventErrorContext`, `EventTimeoutContext`, `CallbackContext` |
| Hatalar | `EventBusError`, `EventBusClosedError`, `InvalidEventHandlerError`, `CallbackTimeoutError`, `EventShutdownTimeoutError` |

`EventFilter` bir event sınıfı veya sınıf tuple'ı ve isteğe bağlı predicate
alır. Yerleşik `MemoryEventHistory(capacity=1000)` sınırlı ve thread-safe'tir;
`capacity` yapılandırılmış kapasiteyi verir. Bus'larda `closed`,
`subscriber_count`, `stats`, `history` ve `error_policy`; engine'lerde
`running` ve `channel_names` bulunur. Abonelik handle'larında `id` ve
`active`, callback policy'de ayrıca `enabled` bulunur.

| Hata | Ne zaman yükseltilir |
| --- | --- |
| `EventBusError` | Bus işlemi veya zamanlama kapasitesi tamamlanamadığında. |
| `EventBusClosedError` | Bus kapandıktan sonra yayın ya da kayıt istendiğinde. |
| `InvalidEventHandlerError` | Handler veya hook seçilen bus'ın sync/async biçimine uymadığında. |
| `CallbackTimeoutError` | Callback için ayrılan süre bütçesi aşıldığında. |
| `EventShutdownTimeoutError` | Senkron periyodik worker `shutdown_timeout` süresinden uzun çalıştığında. |

`BaseEventBus` özel bus uygulamaları için soyut temel sınıftır.
`EventHistory` özel geçmiş depolarının arayüzüdür. Çoğu uygulama yalnızca
`EventBus` veya `AsyncEventBus` kullanabilir.

<a id="development"></a>
## Geliştirme ve lisans

Proje Python 3.10 ve üzerini destekler; paket setuptools ile derlenir.
Uyumluluk bağımlılıkları yalnızca Python 3.10'da kurulur. Geliştirme kurulumu ve kontroller
[Kurulum](https://github.com/TahsinCr/python-eventflow/blob/main/README-TR.md#install)
bölümünde yer alır.
Depo kökünde `python -m benchmarks.benchmark_replay` komutuyla history ve bus
API'leri üzerinden `latest()`/`query()` çağrılarını, ayrıca sınırlı sync/async
replay hazırlığını karşılaştırabilirsiniz. Standart kütüphane ile çalışan araç
varsayılan olarak 100.000 event kullanır; `--events`, `--replay`,
`--iterations` ve `--samples` seçenekleriyle ayarlanır.

Ek ölçümler için `python -m benchmarks.benchmark_publish`,
`python -m benchmarks.benchmark_pipeline` ve `python -m benchmarks.benchmark_stream`
kullanılabilir. `--output results.json`; ortamı, throughput'u, p50/p95/p99
gecikmesini ve zaman ölçümünden ayrı izlenen peak allocation bilgisini kaydeder.
Stream oranları bir tam tüketim işlemindeki öğe sayısıyla hesaplanır. Tracemalloc
değerleri process RSS değildir; yerel mikrobenchmark dağıtım performansı garantisi
vermez. Baz çizgi, son ölçümler ve yöntem `benchmarks/results` altında tutulur.

Copyright © 2026 TahsinCr. EventFlow, MIT Lisansı ile lisanslanmıştır.
[LICENSE](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE) ve
[CHANGELOG](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md)
dosyalarına bakın.
