import { MemoryDB } from '@builderbot/bot';
import { MysqlAdapterCredentials } from '@builderbot/database-mysql/dist/types';
import mysql, { Pool, PoolConnection } from 'mysql2/promise';

class MysqlAdapter extends MemoryDB {
    private pool: Pool;
    private credentials: MysqlAdapterCredentials;
    listHistory: any[] = [];
    private reconnectInterval: NodeJS.Timeout;

    constructor(_credentials: MysqlAdapterCredentials) {
        super();
        this.credentials = _credentials;
        this.pool = mysql.createPool({
            host: _credentials.host,
            user: _credentials.user,
            database: _credentials.database,
            password: _credentials.password,
            port: _credentials.port,
            enableKeepAlive: true,
            keepAliveInitialDelay: 10000,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0
        });
        this.init().then();
        this.pool.on('connection', (connection) => {
            console.log('New connection to database established');
        });

        this.pool.on('enqueue', () => {
            console.log('Waiting for available connection slot');
        });

        // Start the reconnection check interval
        this.reconnectInterval = setInterval(this.checkConnection, 60000); // Check every 60 seconds
    }

    private async getConnection(): Promise<PoolConnection> {
        try {
            const connection = await this.pool.getConnection();
            return connection;
        } catch (error) {
            console.error('Error acquiring connection:', error);
            throw error;
        }
    }

    public async getPrevByNumber(from: string): Promise<any> {
        let connection: PoolConnection | undefined;
        try {
            connection = await this.getConnection();
            const [rows] = await connection.query(
                `SELECT * FROM history WHERE phone = ? ORDER BY id DESC`,
                [from]
            );
            if (Array.isArray(rows) && rows.length > 0) {
                const row = rows[0] as any;
                row.options = JSON.parse(row.options);
                return row;
            }
            return {};
        } catch (error) {
            console.error('Error in getPrevByNumber:', error);
            throw error;
        } finally {
            if (connection) {
                connection.release();
            }
        }
    }

    public async save(ctx: {
        ref: string;
        keyword: string;
        answer: any;
        refSerialize: string;
        from: string;
        options: any;
    }): Promise<void> {
        let connection: PoolConnection | undefined;
        try {
            connection = await this.getConnection();
            const values = [
                ctx.ref,
                ctx.keyword,
                ctx.answer,
                ctx.refSerialize,
                ctx.from,
                JSON.stringify(ctx.options),
            ];
            const sql = `
        INSERT INTO history (ref, keyword, answer, refSerialize, phone, options)
        VALUES (?, ?, ?, ?, ?, ?)
      `;
            await connection.execute(sql, values);
        } catch (error) {
            console.error('Error in save:', error);
            throw error;
        } finally {
            if (connection) {
                connection.release();
            }
        }
    }

    private async createTable(): Promise<void> {
        let connection: PoolConnection | undefined;
        try {
            connection = await this.getConnection();
            const sql = `
        CREATE TABLE IF NOT EXISTS history (
          id INT AUTO_INCREMENT PRIMARY KEY,
          ref varchar(255) DEFAULT NULL,
          keyword varchar(255) DEFAULT NULL,
          answer longtext DEFAULT NULL,
          refSerialize varchar(255) DEFAULT NULL,
          phone varchar(255) NOT NULL,
          options longtext DEFAULT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_General_ci
      `;
            await connection.query(sql);
            console.log('Table history ensured.');
        } catch (error) {
            console.error('Error creating table:', error);
            throw error;
        } finally {
            if (connection) {
                connection.release();
            }
        }
    }

    private async checkTableExists(): Promise<void> {
        let connection: PoolConnection | undefined;
        try {
            connection = await this.getConnection();
            const [rows] = await connection.query("SHOW TABLES LIKE 'history'");
            if ((rows as any[]).length === 0) {
                await this.createTable();
            }
        } catch (error) {
            console.error('Error checking table existence:', error);
            throw error;
        } finally {
            if (connection) {
                connection.release();
            }
        }
    }

    private async init() {
        try {
            await this.checkTableExists();
            console.log('Database initialized and table checked.');
        } catch (error) {
            console.error('Initialization error:', error);
        }
    }

    // Check the connection and reconnect if necessary
    private checkConnection = async () => {
        try {
            // Attempt to get a connection from the pool
            const connection = await this.pool.getConnection();
            connection.release(); // Release immediately after getting it

            console.log('Database connection is healthy.');
        } catch (error) {
            console.error('Database connection is down. Attempting to reconnect...', error);
            this.recreatePool();
        }
    };

    // Recreate the connection pool
    private recreatePool = () => {
        console.log('Recreating connection pool...');
        this.pool.end().then(() => {
            this.pool = mysql.createPool({
                host: this.credentials.host,
                user: this.credentials.user,
                database: this.credentials.database,
                password: this.credentials.password,
                port: this.credentials.port,
                enableKeepAlive: true,
                keepAliveInitialDelay: 10000,
                waitForConnections: true,
                connectionLimit: 10,
                queueLimit: 0
            });

            this.pool.on('connection', (connection) => {
                console.log('New connection to database established after reconnection');
            });

            this.pool.on('enqueue', () => {
                console.log('Waiting for available connection slot after reconnection');
            });

            this.init().then(() => {
                console.log('Reconnected to database successfully.');
            });
        }).catch(err => {
            console.error('Error ending pool during reconnection:', err);
        });
    };

    // Clean up the interval when the adapter is no longer needed (e.g., during shutdown)
    public destroy() {
        clearInterval(this.reconnectInterval);
        this.pool.end();
        console.log('MysqlAdapter destroyed: Reconnection check stopped and connection pool closed.');
    }
}

export { MysqlAdapter };