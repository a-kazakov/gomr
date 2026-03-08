import { promises as fs } from 'fs';
import { join } from 'path';

/**
 * Storage driver interface for persisting pipeline metrics.
 * Allows for different storage backends (filesystem, database, etc.)
 */
export interface StorageDriver {
  /**
   * Saves pipeline metrics data for a given job ID.
   * @param jobId - The unique job identifier
   * @param data - The data to persist (will be serialized as JSON)
   */
  save(jobId: string, data: unknown): Promise<void>;

  /**
   * Loads pipeline metrics data for a given job ID.
   * @param jobId - The unique job identifier
   * @returns The parsed data, or null if not found
   */
  load(jobId: string): Promise<unknown | null>;
}

/**
 * Filesystem-based storage driver.
 * Stores data as JSON files in a directory.
 * 
 * URI format: `fs:/path/to/directory`
 */
export class FileSystemStorage implements StorageDriver {
  private readonly baseDir: string;

  /**
   * Creates a new filesystem storage driver.
   * @param uri - Storage URI in format `fs:/path/to/dir`
   */
  constructor(uri: string) {
    // Parse URI: remove 'fs:' prefix
    if (!uri.startsWith('fs:')) {
      throw new Error(`Invalid filesystem storage URI: ${uri}. Must start with 'fs:'`);
    }
    
    // Extract path (remove 'fs:' prefix and any leading slashes)
    const path = uri.slice(3).replace(/^\/+/, '');
    
    if (!path) {
      throw new Error(`Invalid filesystem storage URI: ${uri}. Path cannot be empty`);
    }

    // Resolve to absolute path
    this.baseDir = path.startsWith('/') ? path : join(process.cwd(), path);
  }

  /**
   * Ensures the storage directory exists.
   * Called during initialization.
   */
  async initialize(): Promise<void> {
    try {
      await fs.mkdir(this.baseDir, { recursive: true });
    } catch (error) {
      throw new Error(`Failed to create storage directory ${this.baseDir}: ${error}`);
    }
  }

  /**
   * Gets the file path for a given job ID.
   */
  private getFilePath(jobId: string): string {
    // Sanitize jobId to prevent directory traversal
    const sanitized = jobId.replace(/[^a-zA-Z0-9_-]/g, '_');
    return join(this.baseDir, `${sanitized}.json`);
  }

  /**
   * Saves data to a JSON file.
   */
  async save(jobId: string, data: unknown): Promise<void> {
    const filePath = this.getFilePath(jobId);
    
    try {
      const jsonData = JSON.stringify(data, null, 2);
      await fs.writeFile(filePath, jsonData, 'utf-8');
    } catch (error) {
      throw new Error(`Failed to save data for job ${jobId}: ${error}`);
    }
  }

  /**
   * Loads data from a JSON file.
   */
  async load(jobId: string): Promise<unknown | null> {
    const filePath = this.getFilePath(jobId);
    
    try {
      const fileContent = await fs.readFile(filePath, 'utf-8');
      return JSON.parse(fileContent);
    } catch (error) {
      // File doesn't exist or is invalid
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return null;
      }
      throw new Error(`Failed to load data for job ${jobId}: ${error}`);
    }
  }
}

/**
 * Factory function to create the appropriate storage driver based on URI.
 * Currently only supports filesystem storage (`fs:` prefix).
 * 
 * @param uri - Storage URI (e.g., `fs:./data`)
 * @returns An initialized storage driver
 */
export async function createStorageDriver(uri: string): Promise<StorageDriver> {
  if (uri.startsWith('fs:')) {
    const driver = new FileSystemStorage(uri);
    await driver.initialize();
    return driver;
  }
  
  throw new Error(`Unsupported storage URI: ${uri}. Currently only 'fs:' prefix is supported.`);
}
