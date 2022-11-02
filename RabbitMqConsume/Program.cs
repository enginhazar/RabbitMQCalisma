using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitmqCalisma;
using System.Text;

public class RabbitMQConsume
{
    public static void Main()
    {

        var factory = new ConnectionFactory() { HostName = "localhost", UserName = "admin", Password = "123456" };
        using (IConnection connection = factory.CreateConnection())
        using (IModel channel = connection.CreateModel())
        {
            //TODO: Received methoduyla gelen dataları yakalayıp işlem yapacağımız için EventingBasicConsumer classından nesne alıyoruz.
            var consumer = new EventingBasicConsumer(channel);
            //TODO: Yeni data geldiğinde bu event otomatik tetikleniyor.
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;//TODO: Kuyruktaki içerik bilgisi.
                var message = Encoding.UTF8.GetString(body.ToArray());//TODO: Gelen bodyi stringe çeviriyoruz.
                Sicil sicil = JsonConvert.DeserializeObject<Sicil>(message); //TODO: Mesajdan dönen veriyi classa çeviriyoruz.
                WriteData(sicil);

            };
            channel.BasicConsume(queue: "Sicil", //TODO: Consume edilecek kuyruk ismi
                autoAck: true, //TODO: Kuyruk ismi doğrulansın mı
                consumer: consumer); //TODO: Hangi consumer kullanılacak.
            Console.WriteLine("Sicil kuyruğuna bağlantı başarılı. Dinleniyor...");
            Console.ReadKey();
        }
    }

    private static void WriteData(Sicil item)
    {
        Console.WriteLine($@"Sicil No    : {item.Sicilno} 
                                      Tc Kimlik no: {item.TcKimlikNo} 
                                      Adı         : {item.Adi} 
                                      Soyadı      : {item.Soyadi}
                                      Doğum Tarihi: {item.DogumTarihi}
                                      Adres       : {item.Adres}
                                      ----------------------------------");
    }
}
