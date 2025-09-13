import axios from 'axios';
import { config } from '../config';

export class QuestDBConnection {
  private baseUrl: string;
  private isConnected = false;

  constructor() {
    this.baseUrl = `http://${config.questdb.host}:${config.questdb.port}`;
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    
    try {
      // Test connection with a simple query
      const response = await axios.get(`${this.baseUrl}/exec?query=SELECT 1`);
      if (response.status === 200) {
        this.isConnected = true;
        console.log('Connected to QuestDB');
      } else {
        throw new Error(`QuestDB connection failed with status: ${response.status}`);
      }
    } catch (error) {
      console.error('Failed to connect to QuestDB:', error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    this.isConnected = false;
    console.log('Disconnected from QuestDB');
  }

  async query(text: string, params?: unknown[]): Promise<unknown> {
    if (!this.isConnected) {
      throw new Error('Database not connected');
    }
    
    try {
      // Replace parameter placeholders ($1, $2, etc.) with actual values
      let query = text;
      if (params && params.length > 0) {
        params.forEach((param, index) => {
          const value = typeof param === 'string' ? `'${param.replace(/'/g, "''")}'` : 
                      param === null ? 'NULL' : 
                      param instanceof Date ? `'${param.toISOString()}'` :
                      String(param);
          // Use a more specific regex to avoid partial matches
          query = query.replace(new RegExp(`\\$${index + 1}\\b`, 'g'), value);
        });
      }

      const response = await axios.get(`${this.baseUrl}/exec`, {
        params: { query }
      });

      if (response.data.error) {
        throw new Error(`QuestDB query error: ${response.data.error}`);
      }

      return response.data;
    } catch (error) {
      console.error('Database query error:', error);
      throw error;
    }
  }

  async executeSchema(): Promise<void> {
    const fs = await import('fs/promises');
    const path = await import('path');
    
    try {
      const schemaPath = path.join(__dirname, 'schema.sql');
      const schema = await fs.readFile(schemaPath, 'utf-8');
      
      // Split by semicolon and execute each statement
      const statements = schema.split(';').filter(stmt => stmt.trim());
      
      for (const statement of statements) {
        if (statement.trim()) {
          await this.query(statement);
        }
      }
      
      console.log('Database schema initialized');
    } catch (error) {
      console.error('Error executing schema:', error);
      throw error;
    }
  }

  async resetAllData(): Promise<void> {
    const tables = ['stock_trades', 'stock_aggregates', 'option_contracts', 'option_trades', 'sync_state'];
    
    for (const table of tables) {
      await this.query(`DROP TABLE IF EXISTS ${table}`);
    }
    
    await this.executeSchema();
    console.log('All data reset and schema recreated');
  }

  getBaseUrl(): string {
    return this.baseUrl;
  }
}

export const db = new QuestDBConnection();
