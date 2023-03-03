const express = require('express');
const app = express();
const connectionString = process.env.REDIS
const Redis = require('redis')
const client = Redis.createClient({
  url: connectionString
});

var cors = require('cors')
app.use(cors())
app.use(express.json());

app.post('/business', async (req, res) => {
  try {
    console.log(req.body);
    const result = await client.json.set(`business:${req.body.id}`, '$', req.body)
    res.status(200).send(result);
  } catch (error) {
    res.status(505).send('Failed')
  }
})

app.post('/order', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const orderPayloads = req.body;

    for (let order of orderPayloads) {
      const query = await client.ft.search('idx:orders', `@fromPhone:${order.fromPhone}`)
      order['status'] = 'pending';
      const result = await client.json.set(`orders:${order.fromPhone}:${query.total + 1}`, '$', order)
      console.log(`orders:${order.fromPhone}:${query.total + 1}`)
    }
    publishEvent('order')
    res.status(200).send(orderPayloads);
  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})

app.get('/order', async (req, res) => {
  try {
    console.log(req.query);

    let condition = ""

    const status = req.query?.status;
    const fromPhone = req.query?.fromPhone;

    if (status) condition += `@status:${status} `
    if (fromPhone) condition += `@fromPhone:${fromPhone} `

    if (!status && !fromPhone) condition = "*"

    const query = await client.ft.search('idx:orders', condition, {
      LIMIT: {
        from: 0,
        size: 100,
      },
      SORTBY: {
        BY: 'timestamp',
        DIRECTION: 'DESC'
      }
    })

    // const query = await client.json.get('orders:*')

    res.status(200).send(query);
  } catch (error) {
    console.error(error);
    res.send('Failed')
  }
})


app.post('/completeOrder', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const orderId = req.body.key;
    console.log('orderId', orderId)
    const result = await client.json.set(orderId, 'status', "completed")
    publishEvent('order')
    res.status(200).send(result);
  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})

app.post('/addProductToOrder', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const orderId = req.body?.order;
    const productId = req.body?.product;
    const quantity = req.body?.quantity;
    if (!productId || !quantity || !orderId) {
      res.status(505).send('Failed')
      return
    }

    const productData = await client.json.get(productId)
    // console.log('productData', productData)
    const productPayload = {
      ...productData,
      'key': productId,
      'name': productData.fullName,
      'quantity': quantity,
      'unitPrice': productData.price,
      'subtotalPrice': productData.price * quantity,
    }

    const result = await client.json.arrAppend(orderId, 'extracted.product', productPayload)
    await client.json.numIncrBy(orderId, 'extracted.totalPrice', productData.price * quantity)
    publishEvent('order')
    res.status(200).send('Ok');

  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})

app.post('/editProductToOrder', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const orderId = req.body?.order;
    const productId = req.body?.product;
    const quantity = req.body?.quantity;
    if (!productId || !quantity || !orderId) {
      res.status(505).send('Failed')
      return
    }

    const productData = await client.json.get(productId)
    // console.log('productData', productData)

    const orderData = await client.json.get(orderId)
    let updatedProductPayload = []
    let totalChange = 0
    for (let product of orderData.extracted.product) {
      if (product.key == productId) {
        totalChange = (productData.price * quantity) - product.subtotalPrice
        product.quantity = quantity;
        product.unitPrice = productData.price;
        product.subtotalPrice = productData.price * quantity;
      }
      updatedProductPayload.push(product)
    }

    const result = await client.json.set(orderId, 'extracted.product', updatedProductPayload)
    await client.json.numIncrBy(orderId, 'extracted.totalPrice', totalChange)
    publishEvent('order')
    res.status(200).send('Ok');

  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})

app.post('/index', async (req, res) => {
  try {

    // await client.ft.dropIndex('idx:products')
    // await client.ft.create('idx:orders', {
    //     '$.status': {
    //         type: Redis.SchemaFieldTypes.TEXT,
    //         SORTABLE: true,
    //         AS: 'status'
    //     },
    //     '$.timestamp': {
    //         type: Redis.SchemaFieldTypes.NUMERIC,
    //         SORTABLE: true,
    //         AS: 'timestamp'
    //     },
    //     '$.fromPhone': {
    //         type: Redis.SchemaFieldTypes.TEXT,
    //         SORTABLE: true,
    //         AS: 'fromPhone'
    //     }

    // }, {
    //     ON: 'JSON',
    //     PREFIX: 'orders'
    // });

    // await client.ft.create('idx:products', {
    //     '$.fullName': {
    //         type: Redis.SchemaFieldTypes.TEXT,
    //         SORTABLE: true,
    //         AS: 'fullName'
    //     },
    //     '$.zh_name': {
    //         type: Redis.SchemaFieldTypes.TEXT,
    //         SORTABLE: true,
    //         AS: 'zh_name'
    //     },
    //     '$.en_name': {
    //         type: Redis.SchemaFieldTypes.TEXT,
    //         SORTABLE: true,
    //         AS: 'en_name'
    //     },
    //     '$.price': {
    //         type: Redis.SchemaFieldTypes.NUMERIC,
    //         SORTABLE: true,
    //         AS: 'price'
    //     }
    // }, {
    //     ON: 'JSON',
    //     PREFIX: 'products'
    // });
    res.status(200).send('Done')
  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})


app.post('/product', async (req, res) => {
  try {
    const productPayloads = []

    for (let product of productPayloads) {
      const query = await client.ft.search('idx:products', '*')
      const result = await client.json.set(`products:${query.total + 1}`, '$', product)
      console.log(`products:${query.total + 1}`)
    }
    publishEvent('product')
    res.status(200).send(productPayloads);
  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})


app.get('/product', async (req, res) => {
  try {
    const query = await client.ft.search('idx:products', '*', {
      LIMIT: {
        from: 0,
        size: 10000,
      }
    })
    res.status(200).send(query);
  } catch (error) {
    console.error(error);
    res.status(505).send('Failed')
  }
})


app.get('/try', async (req, res) => {

  let businessData = await client.json.get('business:shaoye2')
  console.log(businessData)

  res.status(200).send(businessData)
})

const publishEvent = (target) => {
  client.publish("message", target)
}

// app.post('/addCorrectionPair', async (req,res) => {
//     try {
//         console.log(new Date(Date.now()), JSON.stringify(req.body));
//         const businessId = req.body?.business;
//         const target = req.body?.target;
//         const data = req.body?.data;
//         const result = await client.json.set(`business:${businessId}`, target, data)
//         console.log(result)
//         publishEvent('business')
//         res.status(200).send(result)
//     } catch(error) {
//         console.error(error)
//         res.status(505).send('Failed')
//     }
// })

app.get(`/business`, async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const businessId = req.query?.business;
    const result = await client.json.get(`business:${businessId}`)
    console.log(result)
    res.status(200).send(result)
  } catch (error) {
    console.error(error)
    res.status(505).send('Failed')
  }
})


app.post('/editSample', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const businessId = req.body?.business;
    const index = req.body?.index;
    const data = req.body?.data;
    const result = await client.json.set(`business:${businessId}`, `samples[${index}]`, data)
    console.log(result)
    publishEvent('business')
    res.status(200).send(result)
  } catch (error) {
    console.error(error)
    res.status(505).send('Failed')
  }
})


app.post('/addSample', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const businessId = req.body?.business;
    const data = req.body?.data;
    const result = await client.json.arrAppend(`business:${businessId}`, `samples`, data)
    console.log(result)
    publishEvent('business')
    res.status(200).send('Ok')
  } catch (error) {
    console.error(error)
    res.status(505).send('Failed')
  }
})


app.post('/editExtraParameter', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const businessId = req.body?.business;
    const data = req.body?.data;
    const result = await client.json.set(`business:${businessId}`, `extra_parameter`, data)
    console.log(result)
    publishEvent('business')
    res.status(200).send('Ok')
  } catch (error) {
    console.error(error)
    res.status(505).send('Failed')
  }
})

app.post('/editCorrection', async (req, res) => {
  try {
    console.log(new Date(Date.now()), JSON.stringify(req.body));
    const businessId = req.body?.business;
    const data = req.body?.data;
    const result = await client.json.set(`business:${businessId}`, `aiyu_correction`, data)
    console.log(result)
    publishEvent('business')
    res.status(200).send('Ok')
  } catch (error) {
    console.error(error)
    res.status(505).send('Failed')
  }
})

app.listen(3000, async () => {
  console.log('Server started on port 3000');
  await client.connect();
});

process.on("exit", function() {
  client.quit();
});
