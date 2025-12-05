/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  // Entorno de ejecución: node para backend
  preset: 'ts-jest',
  testEnvironment: 'node',
  
  // Mapeo de cómo transformar archivos
  // Le decimos a Jest que use 'ts-jest' para manejar los archivos .ts
  transform: {
    '^.+\\.ts?$': 'ts-jest',
  },
  
  // Dónde encontrar los archivos de prueba
  // Asumiendo que tu archivo de prueba está en 'src/controllers/tests/'
  testMatch: ['**/src/**/*.test.ts'],
  
  // Módulos que ignorar (excluir node_modules)
  transformIgnorePatterns: ['<rootDir>/node_modules/'],
  
  // Opcional: Si tienes un archivo tsconfig.json que necesitas usar
  globals: {
    'ts-jest': {
      tsconfig: 'tsconfig.json',
    },
  },
  
  // Configuración de reportería
  verbose: true,
  collectCoverage: true,
  coverageDirectory: 'coverage',
};