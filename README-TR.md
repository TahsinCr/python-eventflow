[![Sürüm: v1.3.1][version-shield]](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md)
[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![MIT Lisansı][license-shield]](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE)

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
- [Uygulama koordinasyonu](#application-coordination)
  - [Event köprüleri ve kuyruklar](#coordination-bridges)
  - [Task ve kaynak sahipliği](#coordination-lifecycle)
  - [Eşzamanlılık ve yinelenen işler](#coordination-concurrency)
  - [Retry, cooldown ve deadline](#coordination-resilience)
  - [Zamanlama ve sınırlı iş yürütme](#coordination-scheduling)
  - [Durum, cache, servis ve policy](#coordination-state)
  - [İşlem metrikleri](#coordination-metrics)
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

PyPI yayını tamamlandığında EventFlow'u şu komutla kurabilirsiniz:

~~~bash
python -m pip install flowevent
~~~

`flowevent`, yalnızca PyPI dağıtım adıdır. Kütüphanenin adı EventFlow olarak
kalır; Python kodunda `eventflow` paketini içe aktarırsınız:

~~~python
from eventflow import EventBus
~~~

Python 3.10'da, Python 3.11 ve üzerindeki yerleşik özellikleri sağlayan
`backports.strenum` ve `exceptiongroup` paketleri kurulur. Paket metadata'sı bu
bağımlılıkları yalnızca Python 3.10 için etkinleştirir.

Dağıtım, PEP 561 uyumlu `py.typed` işaretçisiyle birlikte tip anotasyonlarını
içerir. Böylece EventFlow kurulduğunda tip denetleyiciler kütüphanenin tiplerini
keşfedebilir.

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

print(__version__)  # 1.3.1
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
`concurrent.futures.ThreadPoolExecutor` handler'ları worker thread'lerinde
çalıştırabilir; ancak yayın çağrısı handler'lar tamamlanana kadar bekler.
Bus'lar aynı pool'u paylaşıyorsa, callback'in aynı pool'u kullanan başka bir bus
üzerinden yaptığı iç içe yayın etkin worker üzerinde doğrudan teslim edilir;
böylece worker'ların birbirini bekleyerek tükenmesi önlenir.

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

Bu sayaçlar her yayın için değişmez bir eşitlik oluşturmaz. `published`, history
kaydı başarılı olduktan (history kapalıysa bu adım atlandıktan) sonra ve
callback'ler çalışmadan önce artar; `delivered` ve `failed` işlem sonunda
güncellenir. Devam eden veya iptal edilen async yayınlarda değerler farklı
kalabilir. `failed`, yalnızca başarısız handler'ları değil; handler, filtre ve
action hatası kayıtlarını sayar.

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
`query()`, `clear()` ve `__len__()` metotlarını uygulayın. Uygulama bu beş
işlemin eşzamanlı çağrılarını kendi içinde senkronize etmeli; sorguları tutarlı
bir snapshot üzerinden yanıtlamalıdır. Aynı geçmiş birden fazla bus tarafından
paylaşılıyorsa senkronizasyon bus'lar arasında da geçerli olmalıdır. Bus'ın
yayın kilidi yalnızca o bus üzerinden yapılan `append()` çağrısını sıralar;
history sorguları bu kilidin dışında çalışır. `append()` hızlı ve bloklamayan
olmalı, aynı bus üzerinden yeniden yayın yapmamalıdır.

Bus üzerindeki `clear()` aktif abonelikleri kaldırır, bekleyen wait işlemlerini
uyandırır; history'yi silmez ve periyodik schedule'ları durdurmaz. Bir schedule'ı
durdurmak için dönen handle'ın `cancel()` metodunu, tüm schedule'ları durdurmak
için bus'ın `close()` metodunu kullanın. Kayıtlı geçmişi silmek için
yapılandırılmış history nesnesinde `clear()` çağırın.

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
çağıran task iptal edilirse her iki kipte de iptal çağırana yayılır. İptal,
handler'ın iptalden önce oluşturduğu yan etkileri geri alamaz.

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

Kanal adları string olmalı; başındaki ve sonundaki boşluklar silinir, harfler
küçültülür. Bu nedenle `" Orders "` ve `"orders"` aynı kanala gider.
`engine.channel(name)` kanalı döndürür veya oluşturur.
`engine.add(name, bus)` özel ayarlı bus kaydeder. `remove(name)` bir kanalı
kapatıp kaldırır; `stop()` tüm kanalları kapatır. Engine ayrıca `running` ve
sıralanmış `channel_names` özelliklerini sunar. Aynı bus örneği aynı anda yalnızca
tek kanal adına kaydedilebilir.
Başarılı `add()` çağrısı bus'ı o engine'e bağlar. Aynı bus örneğini başka bir
engine eklemeye çalışırsa `ValueError` alır. Sahip engine'in `remove()` veya
`stop()` çağrısı bus'ı kapatır.
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
bus'lara aktarır; executor olarak yalnızca `ThreadPoolExecutor` kabul edilir.
`AsyncEventEngine` history, hata politikası, teslimat kipi, `max_concurrency`
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

    stream = (
        AsyncStream(source())
        .map(pipeline.run)
        .filter(lambda value: value % 4 == 0)
        .buffer(2)
        .batch(2)
    )
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
Etkin okuma task'ı, aynı terminal stream'in `aclose()` çağrısını bekleyemez;
karşılıklı beklemeyi önlemek için bu yeniden giriş `RuntimeError` üretir.
Okuma zincirinin hata temizliği stream'i ve upstream kaynakları kapatır.

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
korunur. Async handle'ın `cancel()` metodu awaitable döndürmelidir. `add()` bu
sonucu iptal işlemini başlatmadan doğrulayamadığı için kontrol kapanış sırasında
yapılır. Handle uygulaması tekrarlı iptali güvenli kılmalıdır. Handle'ın kendi
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
`EventEnvelope[T]`; `event`, üretilen `id`, `correlation_id`, `source`, UTC
`timestamp` ve `metadata` alanlarını sunan frozen bir veri sınıfıdır. Verilen
timezone-aware timestamp UTC'ye normalize edilir. Metadata kopyalanıp salt
okunur mapping'e alınır ve `copy.deepcopy()` sonrasında da böyle kalır; payload
ve metadata içindeki nesneler kendi mutability özelliklerini korur. Event ve
metadata değerleri uygunsa `dataclasses.asdict()` ve pickle ile dönüştürülebilir.
Python pickle yükleme sırasında kod çalıştırabildiği için yalnızca güvenilir
kaynaklardan gelen veriyi açın. Payload dönüşümü için `dataclasses.replace`
kullanılabilir.

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
print(stats.snapshot.runs.successful)  # 1
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

<a id="application-coordination"></a>
## Uygulama koordinasyonu

EventFlow, uygulama kodunda kullanılabilecek yürütme ve koordinasyon
primitive'leri de sunar. Her bileşen isteğe bağlıdır; kendi sahibi ve açık bir
kapatma yöntemi vardır. Bileşenler normal Python callable, awaitable, future,
context manager ve iterator arayüzleriyle çalışır. Böylece event teslimatı ve
pipeline sıralaması değişmeden bus, pipeline ve stream'lerle birleştirilebilir.

| Bileşen | Modül | Kullanım amacı |
| --- | --- | --- |
| `AsyncEventQueue`, `LoopPortal` | `eventflow.bridges` | Senkron bus olaylarını ve thread çağrılarını sahip olunan async loop'a taşımak. |
| `AsyncTaskScope` | `eventflow.tasks` | Arka plan asyncio task'larını sahiplenmek. |
| `ResourceScope`, `AsyncResourceScope` | `eventflow.scopes` | Ters sırada, tekrar denenebilir kaynak temizliği yapmak. |
| `KeyedConcurrencyLimiter`, `KeyedRateLimiter`, `SingleFlight` | `eventflow.concurrency` | Anahtarlı iş yükünü sınırlamak, token bucket uygulamak veya devam eden işi paylaşmak. |
| `LatestTaskCoordinator` | `eventflow.tasks` | Her anahtar için yalnız en yeni sonucu uygulamak. |
| `AsyncStream.batch_within()` | `eventflow.stream` | Boyut veya zaman eşiğinde batch üretmek. |
| `BoundedWorkPool` | `eventflow.executors` | Thread pool'a verilen senkron işleri sınırlamak. |
| `KeyedCoalescer`, `AsyncScheduler` | `eventflow.scheduling` | Anahtarlı güncellemeleri birleştirmek ve zamanlanmış async işleri yönetmek. |
| `RetryPolicy`, `CooldownRegistry`, `CircuitBreaker` | `eventflow.resilience` | Retry ve geçici hata davranışını yönetmek. |
| `Deadline` | `eventflow.timing` | İç içe işlemler arasında monotonic zaman bütçesi paylaşmak. |
| `StateMachine`, `VersionedValue` | `eventflow.state` | Yerel durum geçişlerini ve sürümlü değerleri korumak. |
| `VersionedCache` | `eventflow.cache` | Sürüm ve isteğe bağlı TTL içeren bellek içi LRU sınırlandırmak. |
| `ServiceRegistry` | `eventflow.registry` | Sahibi belirlenmiş servisleri cleanup ile kaydetmek. |
| `PolicyEvaluator` | `eventflow.policy` | Sıralı senkron kuralları değerlendirmek. |
| `OperationStats` | `eventflow.observability` | Payload tutmadan sınırlı cardinality ile aggregate toplamak. |

Bu araçlar süreç içi yapı taşlarıdır; durumu kalıcılaştırmaz ve ayrı süreçleri
birbirine koordine etmez. Bileşenler `eventflow` üzerinden lazy top-level API
olarak veya daha dar bir bağımlılık için doğrudan kendi modüllerinden alınabilir.

<a id="coordination-bridges"></a>
### Event köprüleri ve kuyruklar

`AsyncEventQueue`, senkron `EventBus`'a abone olur ve olayları `receive()` veya
async iterator ile sunar. Sabit bir öğe kapasitesi vardır, başka thread'lerden
yayın alabilir ve `drop_oldest`, `drop_newest`, `error` taşma politikalarını
destekler. Kapasite, sahip loop'a aktarılmayı bekleyen event'leri de kapsar.
Aynı anda yalnızca bir consumer task çalışabilir; bu task tamamlandıktan sonra
başka bir task devralabilir. Kuyruk ilk sahip event loop'una bağlı kalır.
Kuyruk kapatıldığında abonelik kaldırılır; içeride bekleyen event'ler tüketilir,
sonraki okuma `EventQueueClosedError` üretir. Sahip loop zaten kapandıysa
`aclose()` senkron bus aboneliğini başka bir çalışan loop'tan kaldırabilir.
`stats()` boyut, teslimat, düşen
event ve taşma hatalarını verir. `error` politikası senkron subscriber içinde
`QueueOverflowError` yükseltir; callback hatasını bus'ın `ErrorPolicy` ayarı
değerlendirir.

~~~python
import asyncio
from eventflow import AsyncEventQueue, EventBus

bus = EventBus[int]()


async def receive_one():
    async with AsyncEventQueue(bus, event_type=int, capacity=64, overflow="drop_oldest") as events:
        await asyncio.to_thread(bus.publish, 42)
        return await events.receive()


assert asyncio.run(receive_one()) == 42
bus.close()
~~~

`LoopPortal` kendisine ait bir event loop'u thread içinde çalıştırır. `submit()`
bir `concurrent.futures.Future` döndürür; başka thread'den senkron sonuç almak
için `call()` kullanılabilir. Gönderim anındaki `contextvars` bağlamı kopyalanır.
`close()` yeni işleri reddeder, portal task'larını iptal edip temizliğini bekler,
varsayılan executor'ı kapatır ve thread'i join eder. Deadlock'ı önlemek için
portal'ın kendi loop'unda bloklayan `call()` reddedilir. Uygulamanın mevcut
event loop'unun sahipliği alınmaz.
`call(timeout=...)` süresi dolarsa task cancellation istenir; cancellation'ı
bastıran bir işlemi zorla durduramaz. İşlemin kendi ürettiği `TimeoutError` aynen
yayılır; portal timeout'u yalnızca `call()` bekleme süresi dolduğunda oluşur.
İş tamamlanana kadar portalın sahipliğinde kalır; bekleme süresi dolarsa `close()`
da timeout verebilir. `asyncio.to_thread()`
ile başlamış bir thread task cancellation ile durdurulamaz. `close()` timeout
verdiğinde portal thread'i bu iş tamamlanana kadar sahipliğinde tutar; ardından
`close()` yeniden çağrılarak kapatma tamamlanabilir.

~~~python
from eventflow import LoopPortal


async def load_value():
    return 42


with LoopPortal() as portal:
    future = portal.submit(load_value)
    assert future.result(timeout=2) == 42
~~~

<a id="coordination-lifecycle"></a>
### Task ve kaynak sahipliği

`AsyncTaskScope`, `start()` ile oluşturulan task'ların sahibidir. Her task için
`shutdown="cancel"` veya `shutdown="drain"` seçilir. Scope kapanırken bu
politika uygulanır, cleanup beklenir ve yeni task başlatılması reddedilir.
`TaskHandle.join()` ilgili task'ın sonucunu döndürür. `snapshot()` aktif,
tamamlanmış, başarısız ve iptal edilmiş task sayılarını verir. `on_error`,
senkron callback olarak join ile alınmamış hataları bildirir.
`aclose(timeout=...)` süresi dolarsa
bitmemiş task'lar sahiplikte kalır; sonraki kapatma çağrısı temizliği yeniden
deneyebilir.

~~~python
from eventflow import AsyncTaskScope


async def refresh_cache():
    pass


def report_task_error(_handle, _error):
    pass


async with AsyncTaskScope(on_error=report_task_error) as tasks:
    refresh = tasks.start(refresh_cache, name="cache-refresh", shutdown="cancel")
    await refresh.join()
~~~

`ResourceScope` ve `AsyncResourceScope`, genel cleanup işlemlerini sahiplenir.
`add(resource, close=..., owned=True)` kapatma metodunu adıyla veya callable
olarak kaydeder; `defer(cleanup)` parametresiz cleanup callback'i ekler.
Temizlik ters kayıt sırasında yürür, bütün kayıtları dener ve başarısız olanları
yeniden denemek üzere tutar. Ödünç alınan kaynakta `owned=False` kullanın;
`detach(resource)` ise sahipliği scope dışına aktarır. `pending_cleanup()`
bekleyen kayıt sayısını verir. Async sürüm async kapatıcıları bekler ve
`async with` sağlar; eşzamanlı kapatma çağrıları aynı cleanup'ı bekler. Bir
cleanup kendi `ResourceScope.close()` metodunu yeniden çağırırsa iç çağrı etkisiz
kalır; sayaç ve cleanup sonuçlarının sahibi dıştaki kapatmadır.

~~~python
from eventflow import ResourceScope


class Resource:
    def close(self):
        pass


def flush_metrics():
    pass


with ResourceScope() as resources:
    resources.add(Resource())
    resources.defer(flush_metrics)
~~~

<a id="coordination-concurrency"></a>
### Eşzamanlılık ve yinelenen işler

`KeyedConcurrencyLimiter(max_concurrency)`, her anahtar için ayrı bir aktif iş
limiti tutar. İptal durumunda slot'u güvenle bırakmak için
`async with limiter.acquire(key)` kullanın. `acquire_nowait(key)` ise hemen
reddedilebilen durumlar içindir. Boştaki anahtar kayıtları temizlenir.
`aclose()` yeni talepleri durdurur ve bekleyenleri uyandırır. Bu bir eşzamanlılık
limitidir; saniyedeki istek sayısını sınırlamaz. Kuyruğa alınan edinimler FIFO
sırasında çalışır. `acquire_nowait(key)`, bekleyen bir iş varken sırayı
atlamaması için `False` döndürür. Başarılı çağrıda slot'u
`await limiter.release(key)` ile bırakın. `stats()` toplam sayıları,
`key_stats(key)` ise tek bir anahtarın aktif izin ve bekleyici sayılarını verir.

`KeyedRateLimiter(rate, period, burst=...)` her anahtar için token bucket
uygular. `await acquire(key)` token'ı iptal edilebilir biçimde bekler;
`try_acquire(key)` hemen sonuç döndürür. Aynı anahtar için `acquire()` beklerken
yeni `try_acquire(key)` çağrısı sıradaki token'ı tüketmemesi için `False`
döndürür. Bekleyen `acquire()` çağrıları arasında kesin FIFO sırası garanti
edilmez. `max_keys` kayıt sayısını sınırlar; tablo doluysa ve temizlenecek boş
bucket yoksa yeni anahtar reddedilir. `prune()` kullanılmayan kayıtları siler.
Loop sahipliğine bağlıdır ve `aclose()` ile kapatılmalıdır.

`SingleFlight.run(key, factory)` aynı anahtar için devam eden tek awaitable'ın
sonucunu çağıranlarla paylaşır. Farklı anahtarlar birbirini beklemez; bir
bekleyicinin iptali ortak işi iptal etmez; mevcut bekleyiciler aynı sonucu veya
hatayı alır. Sonuç tamamlandıktan sonra cache'lenmez. Bütün bekleyiciler ayrılsa
bile ortak iş tamamlanana kadar çalışır ve `stats().active` içinde görünür.
Artık kullanılmayacak örneği `await flight.aclose()` ile kapatın. Bu çağrı
aktif işleri iptal eder, tamamlanmalarını bekler ve yeni işleri reddeder;
kapanışın tamamlanması için factory işlevlerinin iptal isteğine yanıt vermesi
gerekir. Bir factory, kendisini çalıştıran `SingleFlight` örneğini kapatamaz;
bu deneme `RuntimeError` üretir. `stats()` örneğin sahibi olan event loop
içinden çağrılmalıdır.
`LatestTaskCoordinator.submit(key, factory, on_result)` varsayılan olarak
önceki işi iptal eder ve yalnızca en yeni gönderimin sonucunu uygular. İptale
direnen eski task da generation kontrolü sayesinde eski sonuç yayımlayamaz.
`on_result` sahip loop'ta çalışır ve senkron veya awaitable olabilir.
Coordinator `aclose()` ile kapatılır.

~~~python
from eventflow import KeyedConcurrencyLimiter, SingleFlight

limiter = KeyedConcurrencyLimiter[str](max_concurrency=2)
flight = SingleFlight[str]()


async def fetch_account(account_id):
    return {"id": account_id}


async def load_for(account_id):
    async with limiter.acquire(account_id):
        return await flight.run(account_id, lambda: fetch_account(account_id))


await limiter.aclose()
~~~

<a id="coordination-resilience"></a>
### Retry, cooldown ve deadline

`RetryPolicy`; sabit veya exponential backoff, üst gecikme sınırı, isteğe bağlı
jitter ve senkron observer callback'iyle senkron `run()` ve async `run_async()` sunar.
Retry açıkça etkinleştirilir: `should_retry` verilmezse `max_attempts` daha
büyük olsa bile hata ilk denemeden sonra yükseltilir. Senkron predicate normal
`Exception` değerini alır ve `bool` döndürmelidir. Cancellation iletilir ve retry edilmez.
`Deadline` yeniden deneme bütçesidir: süre dolduktan sonra yeni denemeyi engeller
ve retry beklemesini sınırlar. Çalışmaya başlamış operasyonu durdurmaz; başarılı
bir operasyon deadline sonrasında sonuç döndürebilir. Operasyonun çalışma süresi
de sınırlanacaksa etrafına ayrıca cancellation-aware timeout koyun.

`Deadline.after(seconds)` monotonic saat kullanır; `remaining` ve `expired`
değerlerini sunar. `child(seconds)` üst deadline'ı aşamayan daha kısa bir bütçe
oluşturur. `None` süresiz bütçeyi temsil eder. Deadline zaman bütçesini aktarır;
senkron kodu veya cancellation'ı bastıran coroutine'i zorla durduramaz.

`CooldownRegistry`, sınırlı sayıda anahtara ait monotonic bitiş zamanını tutar.
`block()` cooldown koyar veya uzatır; `remaining()` kalan zamanı verir;
`wait()` ve `wait_async()` süre dolana ya da kayıt temizlenene kadar bekleyebilir.
`CircuitBreaker` senkron `call()` veya async `call_async()` çağrılarını `closed`, `open`, `half_open`
durumlarından geçirir. Yarı açık deneme sayısını sınırlar ve task cancellation'ı
başarısızlık saymaz. Bu araçlar tek süreçte çalışır; worker veya makineler
arasında ortak durum oluşturmaz.

~~~python
from eventflow import Deadline, RetryPolicy


class TemporaryError(Exception):
    pass


async def fetch_remote():
    return "response"


deadline = Deadline.after(5)
retry = RetryPolicy(max_attempts=3, initial_delay=0.05, jitter=0.1)
result = await retry.run_async(
    fetch_remote,
    should_retry=lambda error: isinstance(error, TemporaryError),
    deadline=deadline,
)
~~~

<a id="coordination-scheduling"></a>
### Zamanlama ve sınırlı iş yürütme

`AsyncScheduler` tek seferlik ve periyodik asyncio işleri sahiplenir. Periyodik
işler için `overlap="skip"`, `"queue"` veya `"parallel"` seçilebilir;
paralel çalışmanın üst sınırı `max_parallel` ile belirlenir. Tek bir işi iptal
etmek veya beklemek için dönen `ScheduledHandle` saklanabilir. Queue modu callback
çalışırken en fazla bir kaçırılmış tick'i sıraya alır. `aclose()` yeni
schedule'ları durdurur, aktif işleri iptal eder ve cleanup'larını bekler.

`BoundedWorkPool`, thread pool içinde senkron callable'ları çalıştırır ve kabul
edilen iş sayısına sınır koyar.
`max_workers` aynı anda çalışan thread sayısını, `max_pending` worker sayısını
aşan kuyruktaki işleri belirler. `submit()` standart future döndürür; kapasite
doluysa hemen hata verir. Pool `close()` veya context manager ile kapatılır.
Çalışmakta olan senkron fonksiyon thread'den zorla durdurulamaz.

`KeyedCoalescer`, aynı anahtara gelen değerleri verilen merge fonksiyonuyla
birleştirir ve interval sonunda tek worker üzerinden callback'e teslim eder.
İlk değer o anahtarın interval süresini başlatır. `max_pending_keys` saklanan
anahtar sayısını sınırlar. Worker ilk kabul edilen `push()` çağrısında başlar;
kullanılmayan örnek arka plan thread'i oluşturmaz. Key fonksiyonu kilit öncesi,
merge fonksiyonu ise aynı anahtardaki güncellemelerin atomik sırasını korumak
için condition kilidi altında gönderim yapan thread'de çalışır. İkisi de hızlı
ve yeniden giriş yapmayan fonksiyonlar olmalıdır. Teslimat callback'i worker'da
senkron çalışır. `flush()` bekleyen değerleri hemen işler;
`close(flush=True)` önce flush eder, `close(flush=False)` bekleyenleri atar.
Callback hataları `on_error` ve `stats()` üzerinden gözlemlenir.
Eşzamanlı `flush()` çağrıları kendi taleplerini ayrı tutar; bir çağrının
timeout'u diğerinin flush işlemini iptal etmez. İlk `close()` çağrısı flush/at
kararını verir; sonraki çağrılar kapanışı beklemeyi yeniden deneyebilir ancak
kararı değiştirmez. `close(flush=False)` etkin bir flush sürerken işleri atarsa
bu flush `RuntimeError` üretir.

~~~python
from eventflow import BoundedWorkPool


def parse_payload(payload):
    return payload


raw_payload = b"example"
with BoundedWorkPool(max_workers=4, max_pending=8) as workers:
    result = workers.submit(parse_payload, raw_payload).result(timeout=2)
~~~

`AsyncStream.batch_within(max_items, max_delay, partial=True)`, ilk limite
ulaşıldığında batch üretir. Süre sayacı batch'in ilk öğesiyle başlar; öğe başına
task oluşturulmaz. `partial=True` son eksik batch'i verir, `False` ise atar.
Diğer stream işlemleri gibi tek tüketici ve loop sahipliğine bağlıdır; erken
çıkışta açık `aclose()` veya `async with` ile cleanup yapılmalıdır.

~~~python
from eventflow import AsyncStream


async def source():
    for item in range(3):
        yield item


async def store_batch(batch):
    print(batch)


async with AsyncStream(source()).batch_within(100, 0.05) as batches:
    async for batch in batches:
        await store_batch(batch)
~~~

<a id="coordination-state"></a>
### Durum, cache, servis ve policy

`StateMachine(initial, transitions, terminal=..., guard=...)` senkron durum
geçişlerini doğrular. `transition()` durumu ve sürümü atomik değiştirir;
`compare_and_transition(expected_version, target)` başka bir çağrı sürümü
değiştirmişse `None` döndürür. Observer güncelleme sonrasında ve kilit dışında
çalışır. Guard kilit dışında değerlendirilir ve başka geçiş yarışı kazanırsa
yeniden çağrılabilir; senkron ve yan etkisiz olmalıdır. `VersionedValue`, genel
bir değer için aynı snapshot/sürüm ve compare-and-set kalıbını sunar. Snapshot
değeri kopyalamaz; değişebilir nesnenin korunması çağırana aittir.

`VersionedCache`, TTL ve byte limiti seçenekleri olan sınırlı, bellek içi LRU
cache'tir. `max_bytes` verildiğinde `getsizeof(key, value)` de verilmelidir.
`put()` sürüm ister; aynı anahtardaki daha yeni sürüm cache'te durduğu sürece
daha eski yazmayı reddeder. TTL, tahliye veya kaldırma kaydın sürüm geçmişini de
siler; kayıt cache dışında kaldıktan sonra daha eski bir sürüm kabul edilebilir.
Cache dışında monoton sürüm garantisi gerekiyorsa `VersionedValue` veya kalıcı
durum kullanın. Cache yalnızca yerel süreçte geçerlidir; kalıcılık veya süreçler arası
geçersizleştirme sağlamaz.

`ServiceRegistry`, boşlukları kırpılmış ve boş olmayan isimleri açıkça
kaydedilmiş servislerle eşler. Cleanup callback'leri senkron olmalıdır. Her
kayıt bir sahibin altındadır. `close_owner(owner)` bu sahibin kaynaklarını ters
kayıt sırasında kapatır ve başarısız cleanup'ları tekrar
denemek üzere tutar. İsteğe bağlı senkron observer, değişmez kayıt/kaldırma
olaylarını mutasyondan sonra ve registry kilidinin dışında alır. Observer
hatası tamamlanmış değişikliği geri almaz. Servis oluşturmaz ve dependency graph
çözmez.

`PolicyEvaluator`, isimli senkron kuralları kayıt sırasıyla değerlendirir.
Kural `"allow"`, `"deny"`, `"abstain"`, `True`, `False` veya `None` döndürür. Açık deny önceliklidir;
ardından allow, en son ayarlanmış varsayılan karar (varsayılan deny) gelir.
`PolicyDecision` sonucu ve eşleşen kural bilgisini taşır. Bu, kimlik sağlayıcı
veya policy dili değil; küçük, süreç içi bir kural birleştiricisidir.

~~~python
from eventflow import StateMachine, VersionedValue

machine = StateMachine("queued", {"queued": {"running", "cancelled"}})
running = machine.transition("running")
value = VersionedValue("initial")
value.compare_and_set(value.snapshot().version, "ready")
~~~

<a id="coordination-metrics"></a>
### İşlem metrikleri

`OperationStats`; çağrı, sonuç, geçen süre, kuyruk derinliği, düşürülen iş,
retry, ret, timeout ve shutdown süresini thread-safe aggregate olarak toplar.
`observer=` kabul eden bileşenlere aynı nesne verilerek ölçümler tek immutable
snapshot'ta birleştirilebilir. `max_operations` label cardinality'yi sınırlar;
limit dışındaki isimler `other` kovasında birikir. `reset()` sayaçları temizler.
Metrikler yalnız sayaçları tutar; event payload, callback sonucu veya exception
nesnesi saklamaz. Adları sabit tutun; telemetry gönderimini uygulama tarafındaki
sınırlı kuyruğa bırakın.

~~~python
from eventflow import KeyedConcurrencyLimiter, OperationStats


async def update_account():
    pass


metrics = OperationStats(max_operations=64)
limiter = KeyedConcurrencyLimiter[str](1, observer=metrics)
async with limiter.acquire("account-42"):
    await update_account()
print(metrics.snapshot().operations["keyed_concurrency.acquire"].successful)
await limiter.aclose()
~~~

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

Depo kökünden çalıştırılabilen üç tam örnek bulunur:

~~~bash
python -m examples.integrated
python -m examples.bus_stream
python -m examples.coordination
~~~

[integrated.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/integrated.py);
sync/async stage'leri, decorator'ları, callback politikalarını, korelasyonu,
ölçümleri, bounded stream'i ve async bus'ı bir araya getirir.
[bus_stream.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/bus_stream.py)
ise sınırlı bir uygulama kuyruğunu bus aboneliği ve stream kaynağı olarak kullanır.
Tek publisher task'ını sahiplenip çıkışta bekler; her event için yeni task açmaz.
[coordination.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/coordination.py)
senkron bus, `AsyncEventQueue`, `AsyncTaskScope`, anahtarlı eşzamanlılık limiti,
açık retry, deadline ve ortak işlem metriklerini birleştirir.

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
yükseltir. `clear()` abonelikleri kaldırır, history'yi silmez ve periyodik
schedule'ları durdurmaz; schedule handle'ını iptal edin veya bus'ı kapatın.
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
| `clear()` / `close()` | Abonelikleri temizler (schedule'lar sürer) veya bus'ı kapatır (schedule'lar durur). `AsyncEventBus` üzerinde await edilir. |

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
| Koordinasyon ve yaşam döngüsü | `AsyncEventQueue`, `AsyncTaskScope`, `ResourceScope`, `AsyncResourceScope`, `LoopPortal`, `TaskHandle` |
| Eşzamanlılık ve zamanlama | `KeyedConcurrencyLimiter`, `KeyedConcurrencyKeyStats`, `KeyedRateLimiter`, `SingleFlight`, `LatestTaskCoordinator`, `AsyncScheduler`, `KeyedCoalescer`, `BoundedWorkPool` |
| Durum ve dayanıklılık | `StateMachine`, `VersionedValue`, `VersionedCache`, `ServiceRegistry`, `RegistryChange`, `PolicyEvaluator`, `RetryPolicy`, `CooldownRegistry`, `CircuitBreaker`, `Deadline` |
| Stream ve metrikler | `AsyncStream`, `OperationStats`, `OperationMetrics`, `PipelineStats`, `EventEnvelope` |
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
| `EventQueueClosedError` | Kapanmış kuyruk boşaldıktan sonra okuma istendiğinde. |
| `QueueOverflowError` | `overflow="error"` ayarlı `AsyncEventQueue` dolduğunda. |
| `LoopPortalClosedError` | Portal kapandıktan sonra iş gönderildiğinde. |
| `CircuitOpenError` | Circuit açıkken veya yarı açık probe sınırına ulaşıldığında. |
| `StateTransitionError` | Tanımlı geçiş grafiğinin izin vermediği bir geçiş istendiğinde. |

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
`python -m benchmarks.benchmark_pipeline`, `python -m benchmarks.benchmark_stream`
ve `python -m benchmarks.benchmark_runtime` kullanılabilir. Stream benchmark'ı
burst girişte `batch_within(64, 60)` ile yerel size-only batching'i karşılaştırır;
bu ölçüm batch coordination maliyetini kapsar, seyrek kaynakta timer flush
gecikmesini ölçmez. Runtime benchmark'ı
sync/async publisher rekabetini, async `max_concurrency` değerlerini, callback
hata oranlarını, periyodik schedule kaynaklarını ve ayarlanabilir sayıda event
üzerinden izlenen belleği karşılaştırır. Varsayılan olarak beş ölçüm örneği,
her örnekte publisher başına 500 yayın ve 100.000 soak eventi kullanır; iş yükü
`--iterations`, `--publisher-counts`, `--schedule-counts`, `--samples` ve
`--soak-operations` seçenekleriyle ayarlanır.
`--output results.json` ortamı, throughput'u, p50/p95/p99 gecikmeyi ve zaman
ölçümünden ayrı izlenen peak allocation değerini kaydeder. Stream oranları tam
tüketilen bir stream'deki öğe sayısına göredir. Runtime bellek ölçümleri
`tracemalloc` kullanır, process RSS değildir ve history belleğini de içerir.
Yerel mikrobenchmark dağıtım performansı garantisi vermez. Ölçüm sonuçları ve
yöntem `benchmarks/results` altında tutulur.

Copyright © 2026 TahsinCr. EventFlow, MIT Lisansı ile lisanslanmıştır.
[LICENSE](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE) ve
[CHANGELOG](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md)
dosyalarına bakın.

[version-shield]: https://img.shields.io/badge/version-v1.3.1-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
