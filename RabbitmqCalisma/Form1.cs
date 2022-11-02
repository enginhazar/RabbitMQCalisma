using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

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

            foreach (var item in Sicil.GenerateSicil(baslangic))
            {
                RabbitMQHelper.AddQueue(item);
            }
            baslangic+=2000;
            
        }

        private void button2_Click(object sender, EventArgs e)
        {
            List<Sicil> sicils = RabbitMQHelper.ConsumeQueue();

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
    }

    public class RabbitMQHelper
    {
        public static void  AddQueue(object model)
        {
            var factory = new ConnectionFactory() 
            { HostName = "localhost",
                UserName = "admin",
                Password = "123456" };
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

        public static List<Sicil> ConsumeQueue()
        {
            List<Sicil> ret = new List<Sicil>();
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
                    ret.Add(sicil);
                    
                };
                channel.BasicConsume(queue: "Sicil", //TODO: Consume edilecek kuyruk ismi
                    autoAck: true, //TODO: Kuyruk ismi doğrulansın mı
                    consumer: consumer); //TODO: Hangi consumer kullanılacak.
                return ret;
            }

        }
    }

    public class Sicil
    {
        public int Sicilno { get; set; }
        public Int64 TcKimlikNo { get; set; }
        public string Adi { get; set; }
        public string Soyadi { get; set; }
        public string Adres { get; set; }

        public DateTime DogumTarihi { get; set; }

        public static List<Sicil> GenerateSicil(int baslangic)
        {
            List<Sicil> list = new List<Sicil>();
            for (int i = baslangic; i < baslangic+2000; i++)
            {
                list.Add(new Sicil()
                {
                    Sicilno = i,    
                    TcKimlikNo=12345678901+i,
                    DogumTarihi=new DateTime(2000+i%10,1,1,10,12,45),
                    Adi=i%2==0? $"üğişçöÜĞŞİÇÖ{i}":$"Defne_{i}",
                    Soyadi=$"Hazar_{i}",
                    Adres="Yüzüncü Yıl Mahallesi Prof Dr.Erdal İnönü Caddesi Günce Sitesi Kat:10 D:20"

                });
            }
            return list;
        }
    }
}
