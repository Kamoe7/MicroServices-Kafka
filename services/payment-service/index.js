import express from 'express';
import cors from 'cors';
import { Kafka } from 'kafkajs';

const app = express();

app.use(cors({origin: 'http://localhost:3000'}));
app.use(express.json());

const kafka=  new Kafka({
    clientId: 'payment-service',
    brokers: ['localhost:9094','localhost:9095','localhost:9096']
});

const producer = kafka.producer();

const connectToKafka = async()=> {
    try{
        await producer.connect();
        console.log('Connected to Kafka');
    }
    catch (error) {
        console.error('Error connecting to Kafka:', error);
    }
}

app.post('/payment-service',async (req,res)=>{
    const {cart}= req.body;

    //ASSUME THAT ER GET THE COOKIE AND DECRYPT IT TO GET THE USER ID
    const userId="123456";
    //Todo: payment
    console.log("Payment initiated for user:", userId);
    //kafka

    await producer.send({
        topic:'payment-service',
        messages: [{value:JSON.stringify({userId,cart})}]
    });


    return res.status(200).send("Payment successful");

});




app.use((err,req,res,next)=>{
    res.status(err.status || 500).send(err.message)
})

app.listen(8000,()=>{
    connectToKafka();
    console.log('Payment service is running on port 8000');
});


