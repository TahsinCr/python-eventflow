[English](https://github.com/TahsinCr/python-eventflow/blob/main/README.md) | **Türkçe**

# EventFlow

[Depo](https://github.com/TahsinCr/python-eventflow) · [Son sürüm](https://github.com/TahsinCr/python-eventflow/releases/latest) · [MIT Lisansı](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE)

EventFlow, aynı Python sürecindeki bileşenlerin sıradan Python nesneleri
üzerinden haberleşmesini sağlar. Senkron bir event bus, asyncio tabanlı bir bus,
isteğe bağlı bellek içi geçmiş, isimlendirilmiş kanallar ve aboneliklerin açık
yaşam döngüsü yönetimini sunar.

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

Python 3.10 veya üzeri gerekir. Depo kök dizininde paketi etkin Python
ortamına kurun:

~~~bash
python -m pip install .
~~~

Dağıtımın adı `eventflow`, import edilen paketin adı `eventflow` olur. Python
3.10'da, Python 3.11 ve üzerindeki yerleşik özellikleri sağlayan
`backports.strenum` ve `exceptiongroup` paketleri kurulur. Bu bağımlılıklar
paket metadata'sında yalnızca Python 3.10 için etkinleştirilmiştir.

`eventflow` adı şu anda PyPI'de başka bir projeye kayıtlıdır
([PyPI kaydı](https://pypi.org/project/eventflow/)). Bu nedenle
`python -m pip install eventflow` bu kütüphaneyi kurmaz. Şimdilik yukarıdaki
kaynak koddan kurulum komutunu kullanın. Bu dağıtım adıyla yayımlama için adın
yetkili biçimde devredilmesi veya kullanılabilir başka bir ad seçilmesi gerekir;
Python import paketi her iki durumda da `eventflow` olarak kalabilir.

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

print(__version__)  # 1.0
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
| `matched` | Bu event için seçilen abonelik sayısı |
| `delivered` | Başarıyla tamamlanan handler sayısı |
| `failed` / `errors` | Toplanan teslimat veya action hataları |
| `successful` | Hata toplanmadıysa `True` |

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
periyodik yayın sırasında hata oluşursa ilgili zamanlama durur.

<a id="async-applications"></a>
## Async uygulamalar

`AsyncEventBus` async handler ve async bus hook'ları kabul eder.
Varsayılan `DeliveryMode.SEQUENTIAL`, eşleşen handler'ları abonelik sırasıyla
tek tek await eder. `DeliveryMode.CONCURRENT` handler'ları birlikte bekler.
Handler'lar birbirinden bağımsızsa concurrent kip bekleme süresini azaltabilir;
tamamlanma sırası garanti edilmez.
Sıralı teslimat garantisi tek bir `publish()` çağrısı içindedir; ayrı yayın
çağrıları birbiriyle çakışabilir. Concurrent kipte `asyncio.CancelledError` ile
sonlanan bir handler, `PublishResult` içinde başarısız teslimat olarak raporlanır.
`publish()` çağrısını yapan task iptal edilirse iptal çağırana yayılır ve devam
eden handler task'ları iptal edilir.

~~~python
import asyncio

from eventflow import AsyncEventBus, DeliveryMode


async def main() -> None:
    async with AsyncEventBus[int](
        delivery_mode=DeliveryMode.CONCURRENT,
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
bus'lara aktarır. `AsyncEventEngine` history, hata politikası, teslimat kipi
ve async hook'ları aktarır.

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
Async timeout callback task'ını iptal eder. Callback'in kendi
`TimeoutError` hatası, `CallbackTimeoutError` ile aynı değildir.

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

Copyright © 2026 TahsinCr. EventFlow, MIT Lisansı ile lisanslanmıştır.
[LICENSE](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE) ve
[CHANGELOG](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md)
dosyalarına bakın.
