import "reflect-metadata";
import express from "express";
import { AppDataSource } from "./database/data-source";
import transactionRoutes from "./routes/transactionRoutes";

const app = express();
const port = 3000;

app.use(express.json());

AppDataSource.initialize()
  .then(async () => {
    console.log("Database connection initialized successfully.");

    const app = express();
    app.use(express.json());

    // Rutas
    app.use("/transaction", transactionRoutes);

    // Iniciar el servidor
    const PORT = 3000;
    app.listen(PORT, () => {
      console.log(`Server running on http://localhost:${PORT}`);
    });
  })
  .catch((error) =>
    console.error("Error during Data Source initialization:", error)
  );
