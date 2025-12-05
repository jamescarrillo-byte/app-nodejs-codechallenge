import { Kafka, ITopicConfig } from "kafkajs";
import { Transaction } from "../entities/Transaction";
import { AppDataSource } from "../database/data-source";

// --- Configuración de Kafka ---
const kafka = new Kafka({
  clientId: "anti-fraud-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const TRANSACTION_CREATED_TOPIC = "transaction-created";
const TRANSACTION_STATUS_TOPIC = "transaction-status-updated";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function ensureTopicsExist() {
  const admin = kafka.admin();
  try {
    await admin.connect();

    console.log("[Kafka Admin] Conectado para verificar tópicos.");

    const topicsToCreate: ITopicConfig[] = [
      {
        topic: TRANSACTION_CREATED_TOPIC,
        numPartitions: 1,
        replicationFactor: 1,
      },
      {
        topic: TRANSACTION_STATUS_TOPIC,
        numPartitions: 1,
        replicationFactor: 1,
      },
    ];

    const existingTopics = await admin.listTopics();

    const topicsNeedingCreation = topicsToCreate.filter(
      (t) => !existingTopics.includes(t.topic)
    );

    if (topicsNeedingCreation.length > 0) {
      console.log(
        `[Kafka Admin] Creando ${topicsNeedingCreation.length} tópicos faltantes...`
      );
      await admin.createTopics({
        topics: topicsNeedingCreation,
        waitForLeaders: true,
      });
      console.log("[Kafka Admin] Tópicos creados exitosamente.");
    } else {
      console.log("[Kafka Admin] Todos los tópicos requeridos ya existen.");
    }
  } catch (error) {
    console.error("[Kafka Admin] Error al asegurar tópicos:", error);
    throw error;
  } finally {
    await admin.disconnect();
  }
}

export async function connectKafkaProducer() {
  const MAX_RETRIES = 5;
  let attempt = 0;

  while (attempt < MAX_RETRIES) {
    try {
      // 1. Asegurar la existencia de los tópicos
      await ensureTopicsExist();

      // 2. Conectar el productor
      console.log("[Kafka] Conectando Producer...");
      await producer.connect();
      console.log("[Kafka] Producer conectado exitosamente.");
      return; // Éxito
    } catch (error) {
      attempt++;

      const nextDelay = 2000 * Math.pow(2, attempt - 1);

      console.warn(
        `[Kafka] Intento ${attempt} fallido. Reintentando en ${
          nextDelay / 1000
        }s. Error: ${(error as any).message}`
      );

      if (attempt >= MAX_RETRIES) {
        console.error(
          "[Kafka] Error FATAL: Máximo de reintentos alcanzado. No se pudo conectar a Kafka."
        );
        throw error;
      }

      await delay(nextDelay);
    }
  }
}

/**
 * publicador de mensajes para procesar
 * @param transaction 
 */
export async function publishTransactionCreatedEvent(transaction: Transaction) {
  try {
    // mapping message to send
    const message = {
      transactionExternalId: transaction.transactionExternalId,
      value: transaction.value,
    };

    await producer.send({
      topic: TRANSACTION_CREATED_TOPIC,
      messages: [
        {
          key: transaction.transactionExternalId,
          value: JSON.stringify(message),
        },
      ],
    });

    console.log(
      `[Kafka] Evento 'Transaction Created' enviado para ID: ${transaction.transactionExternalId}`
    );
  } catch (error) {
    console.error("[Kafka] Error publicando el evento:", error);
  }
}

/**
 * Consumidor 1: Simula micro Anti-fraud.
 * Aplica regla (value > 1000) y produce el resultado
 */
export async function startAntiFraudService() {
  const ANTI_FRAUD_GROUP_ID = "anti-fraud-processor";
  const consumer = kafka.consumer({ groupId: ANTI_FRAUD_GROUP_ID });

  await consumer.connect();
  // Suscripción al tópico de transacciones creadas
  await consumer.subscribe({
    topic: TRANSACTION_CREATED_TOPIC,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const transactionData = JSON.parse(message.value?.toString() || "{}");
      console.log(
        `[Anti-Fraud] Evaluando transacción: ${transactionData.transactionExternalId} (Valor: ${transactionData.value})`
      );

      let newStatus = "approved";

      // REGLA DE NEGOCIO
      if (transactionData.value > 1000) {
        newStatus = "rejected";
      }

      // Enviar evento de actualización de estado (publicar al segundo tópico)
      await producer.send({
        topic: TRANSACTION_STATUS_TOPIC,
        messages: [
          {
            key: transactionData.transactionExternalId,
            value: JSON.stringify({
              transactionExternalId: transactionData.transactionExternalId,
              newStatus: newStatus,
            }),
          },
        ],
      });

      console.log(
        `[Anti-Fraud] Decisión para ${transactionData.transactionExternalId}: ${newStatus}. Publicando a ${TRANSACTION_STATUS_TOPIC}`
      );
    },
  });
  console.log(
    '[Kafka Consumer] Anti-Fraud Service iniciado, escuchando en el tópico "transaction-created".'
  );
}

/**
 * Consumidor 2: Actualiza el estado de la DB.
 * Escucha 'transaction-status-updated' y actualiza el campo transactionStatus.
 */
export async function startStatusConsumer() {
  const STATUS_UPDATE_GROUP_ID = "transaction-status-group";
  const consumer = kafka.consumer({ groupId: STATUS_UPDATE_GROUP_ID });

  const transactionRepository = AppDataSource.getRepository(Transaction);

  await consumer.connect();

  // Suscripción al tópico de estado actualizado
  await consumer.subscribe({
    topic: TRANSACTION_STATUS_TOPIC,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value?.toString() || "{}");

      const { transactionExternalId, newStatus } = data;

      if (transactionExternalId && newStatus) {
        // Ejecutar la actualización en la base de datos
        await transactionRepository.update(
          { transactionExternalId },
          { transactionStatus: newStatus }
        );
        console.log(
          `[DB Updater] Estado de ${transactionExternalId} actualizado a ${newStatus}`
        );
      }
    },
  });
  console.log(
    '[Kafka Consumer] DB Status Updater iniciado, escuchando en el tópico "transaction-status-updated".'
  );
}