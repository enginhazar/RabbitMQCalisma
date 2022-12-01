using Nest;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitmqCalisma.Properties;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Windows.Forms;
using static System.Net.Mime.MediaTypeNames;

namespace RabbitmqCalisma
{
    /// <summary>
    ///  docker run -d -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=123456 -p 1453:15672 -p 5672:5672 --name rabbitmqcontainer rabbitmq:3-management
    /// </summary>
    public partial class Form1 : Form
    {
        int baslangic = 0;
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {

            foreach (var item in Personel.GenerateSicil(baslangic))
            {
                RabbitMQHelper.AddQueue(item);
            }
            baslangic+=2000;

        }

        private void button2_Click(object sender, EventArgs e)
        {
            List<Personel> sicils = RabbitMQHelper.ConsumeQueue();

            foreach (var item in sicils)
            {
                richTextBox1.Text+=$@"Sicil No    : {item.Sicilno} 
                                      Tc Kimlik no: {item.TcKimlikNo} 
                                      Adı         : {item.Adi} 
                                      Soyadı      : {item.Soyadi}
                                      Doğum Tarihi: {item.DogumTarihi}
                                      Adres       : {item.Adres}
                                      ----------------------------------";
            }
        }

        private void button3_Click(object sender, EventArgs e)
        {
            ElasticSearchNestHelper.AddPersonel(new Personel() { Adi="engin", Soyadi="hazar",Adres="Yüzüncüyıl",DogumTarihi=new DateTime(1979,02,13),Sicilno=1,TcKimlikNo=37396047610 });
        }

        private void button4_Click(object sender, EventArgs e)
        {
           // ElasticSearchNestHelper.CreateIndex("Personel");
        }

        private void button5_Click(object sender, EventArgs e)
        {
         //   ElasticSearchHelper.GetAllData("Personel");
        }
    }

    public class RabbitMQHelper
    {
        public static void AddQueue(object model)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "admin",
                Password = "123456"
            };
            //Channel yaratmak için
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                //Kuyruk oluşturma
                channel.QueueDeclare(queue: "Sicil",
                    durable: false, //Data fiziksel olarak mı yoksa memoryde mi tutulsun
                    exclusive: false, //Başka connectionlarda bu kuyruğa ulaşabilsin mi
                    autoDelete: false, //Eğer kuyruktaki son mesaj ulaştığında kuyruğun silinmesini istiyorsak kullanılır.
                    arguments: null);//Exchangelere verilecek olan parametreler tanımlamak için kullanılır.

                string message = JsonConvert.SerializeObject(model);
                var body = Encoding.UTF8.GetBytes(message);


                //Queue ya atmak için kullanılır.
                channel.BasicPublish(exchange: "",//mesajın alınıp bir veya birden fazla queue ya konmasını sağlıyor.
                    routingKey: "Sicil", //Hangi queue ya atanacak.
                    body: body);//Mesajın içeriği
            }
        }

        public static List<Personel> ConsumeQueue()
        {
            List<Personel> ret = new List<Personel>();
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
                    Personel sicil = JsonConvert.DeserializeObject<Personel>(message); //TODO: Mesajdan dönen veriyi classa çeviriyoruz.
                    ret.Add(sicil);

                };
                channel.BasicConsume(queue: "Personel", //TODO: Consume edilecek kuyruk ismi
                    autoAck: true, //TODO: Kuyruk ismi doğrulansın mı
                    consumer: consumer); //TODO: Hangi consumer kullanılacak.
                return ret;
            }

        }
    }


    public  class ElasticSearchNestHelper
    {

         static ConnectionSettings setttings = new ConnectionSettings(new Uri("http://localhost:9200"))
            .BasicAuthentication("elastic", "123456");
        static ElasticClient client =new ElasticClient(setttings);
        

        public static void AddPersonel(Personel personel)
        {
            if (!client.Indices.Exists("personel").Exists)
                CreateIndex();
            var response=client.Index<Personel>(personel,ind=> ind.Index("personel"));
            if (!response.IsValid)
            {
                Console.WriteLine(response.ToString()); 
            }
        }

        private static void CreateIndex()
        {
            client.Indices.Create("personel", c1 =>
            c1.Index("personel")
            .Mappings(ms=> ms.Map<Personel>(m=> m.AutoMap()))
            .Aliases(a=> a.Alias("personel"))
            .Settings(s=> s.NumberOfShards(3).NumberOfReplicas(1)
            ));
        }

    }

    public class Personel
    {
        public int Sicilno { get; set; }
        public Int64 TcKimlikNo { get; set; }
        public string Adi { get; set; }
        public string Soyadi { get; set; }
        public string Adres { get; set; }

        public DateTime DogumTarihi { get; set; }

        public static List<Personel> GenerateSicil(int baslangic)
        {
            List<Personel> list = new List<Personel>();
            for (int i = baslangic; i < baslangic+2000; i++)
            {
                list.Add(new Personel()
                {
                    Sicilno = i,
                    TcKimlikNo=12345678901+i,
                    DogumTarihi=new DateTime(2000+i%10, 1, 1, 10, 12, 45),
                    Adi=i%2==0 ? $"üğişçöÜĞŞİÇÖ{i}" : $"Defne_{i}",
                    Soyadi=$"Hazar_{i}",
                    Adres="Yüzüncü Yıl Mahallesi Prof Dr.Erdal İnönü Caddesi Günce Sitesi Kat:10 D:20"

                });
            }
            return list;
        }
    }
}
