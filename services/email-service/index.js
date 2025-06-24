import {Kafka} from 'kafkajs';

const kafka = new Kafka({
    clientId:'email-service',
    brokers:'localhost:9094'
});

const producer = kafka.producer();
const consumer = kafka.consumer({groupId:'email-service'});

const run = async() =>{
    try{

        await producer.connect();
        await consumer.connect();
        
        await consumer.subscribe({
            topic:'order-successful',
            fromBeginning: true,
        })

        await consumer.run({
            eachMessage: async ({topic, partition, message})=>{
                const value = message.value.toString();
                const {userId, orderID}= JSON.parse(value);
                
                // TODO send email to user
                const dummyEmailId = '98656556656';
                console.log(`Email sent to user ${userId} for order ${orderID}. Email ID: ${dummyEmailId}`);

                await producer.send({
                    topic:'email-successful',
                    messages:[
                        {
                            value:JSON.stringify({
                                userId, emailId:dummyEmailId
                            })
                        }
                    ]
                })
            }
        })

    }catch(error) {
        console.error('Error connecting to Kafka:', error);
    }
}

run();