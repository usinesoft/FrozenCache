using MessagePack;

namespace UnitTests.TestData
{
    [MessagePackObject]
    public class Invoice
    {
        [Key(0)]
        public int Id { get; set; }
        
        [Key(1)]
        public string? ClientName { get; set; }

        [Key(2)]
        public DateOnly Date { get; set; }

        [Key(3)]
        public DateOnly? PaymentDate { get; set; }

        [IgnoreMember]
        public bool IsPayed => PaymentDate.HasValue;

        [Key(4)]
        public List<InvoiceLine> Lines { get; set; } = [];

        public static IEnumerable<Invoice> GenerateInvoices(int count)
        {
            DateTime startDate = DateTime.Today;

            Random rg = new Random();

            for (int i = 1; i <= count; i++)
            {
                var invoice = new Invoice
                {
                    Id = i * 2,
                    Date = DateOnly.FromDateTime(startDate),
                    PaymentDate = i % 10 != 0? DateOnly.FromDateTime(startDate.AddDays(3)) : null,
                    ClientName = $"Client {rg.Next(1, 1000)}",
                };

                var linesCount = rg.Next(10);

                for (int j = 0; j < linesCount; j++)
                {
                    var invoiceLine = new InvoiceLine
                    {
                        ProductId = rg.Next(1, 1000),
                        ProductName = $"Product {rg.Next(1, 1000)}",
                        Quantity = rg.Next(1, 20),
                        UnitPrice = Math.Round((decimal)(rg.NextDouble() * 100), 2)
                    };
                    invoice.Lines.Add(invoiceLine);
                }



                yield return invoice;
            }
        }
    }

    

    [MessagePackObject]
    public class InvoiceLine
    {
        [Key(0)]
        public long ProductId { get; set; }
        
        [Key(1)]
        public string ProductName { get; set; }

        [Key(2)]
        public decimal UnitPrice { get; set; }

        [Key(3)]
        public int Quantity { get;set; }

        [IgnoreMember]
        public decimal Price => UnitPrice * Quantity;
    }
}
