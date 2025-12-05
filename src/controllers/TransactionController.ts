import { Request, Response } from "express";
import { AppDataSource } from "../database/data-source";
import { Transaction } from "../entities/Transaction";

// Servicio para crear una nueva transacción (POST)
export const createTransaction = async (req: Request, res: Response) => {
  const {
    accountExternalIdDebit,
    accountExternalIdCredit,
    tranferTypeId,
    value,
  } = req.body;

  if (
    !accountExternalIdDebit ||
    !accountExternalIdCredit ||
    !tranferTypeId ||
    value === undefined
  ) {
    return res.status(400).json({ message: "Missing required fields" });
  }

  const transactionRepository = AppDataSource.getRepository(Transaction);

  try {
    // 1. Save Transaction with pending Status
    let newTransaction = transactionRepository.create({
      accountExternalIdDebit,
      accountExternalIdCredit,
      tranferTypeId,
      value: parseFloat(value), // Aseguramos que sea número
      transactionStatus: "pending",
    });

    newTransaction = await transactionRepository.save(newTransaction);

    return res.status(201).json({
      message: "Transaction created and pending validation",
      transactionExternalId: newTransaction.transactionExternalId,
    });
  } catch (error) {
    console.error("Error creating transaction:", error);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};

// Servicio para obtener una transacción por ID (GET)
export const getTransactionById = async (req: Request, res: Response) => {
  const { id } = req.params; // Capturamos el ID de la ruta

  if (!id) {
    return res.status(400).json({ message: "Transaction ID is required" });
  }

  const transactionRepository = AppDataSource.getRepository(Transaction);

  try {
    // Buscamos la transacción por su ID externo
    const transaction = await transactionRepository.findOne({
      where: { transactionExternalId: id }
    });

    if (!transaction) {
      return res.status(404).json({ message: "Transaction not found" });
    }

    return res.status(200).json({
      transactionExternalId: transaction.transactionExternalId,
      transactionType: {
        name: transaction.tranferTypeId,
      },
      transactionStatus: {
        name: transaction.transactionStatus, // 'pending', 'approved', 'rejected'
      },
      value: transaction.value,
      createdAt: transaction.createdAt,
    });
  } catch (error) {
    console.error("Error retrieving transaction:", error);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};
