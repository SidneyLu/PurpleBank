/**
 * Shared frontend TypeScript types aligned with backend response schemas.
 */

export type UserRole = "user" | "admin";

export interface UserProfile {
  id: number;
  username: string;
  role: UserRole;
  is_active: boolean;
  created_at?: string;
}

export interface AuthTokenResponse {
  access_token: string;
  token_type: string;
  user: UserProfile;
}

export interface SequenceListItem {
  accession: string;
  version?: string;
  locus?: string;
  scientific_name?: string;
  genus?: string;
  seq_status_desc: string;
  seq_length?: number;
  submit_time?: string;
  operate_count: number;
}

export interface SequenceSearchResponse {
  items: SequenceListItem[];
  page: number;
  page_size: number;
  total: number;
}

export interface SequenceDetail {
  accession: string;
  version?: string;
  locus?: string;
  definition?: string;
  organism_id?: number;
  scientific_name?: string;
  genus?: string;
  seq_status?: number;
  seq_status_desc?: string;
  submitter?: string;
  submit_time?: string;
  seq_length?: number;
  sequence?: string;
  features: Record<string, unknown>[];
  references: Record<string, unknown>[];
}

export type SequenceActionType = "CREATE" | "UPDATE" | "DELETE";
export type RequestStatus = "PENDING" | "APPROVED" | "REJECTED";

export interface SequenceRequestItem {
  id: number;
  action_type: SequenceActionType;
  target_accession: string;
  payload_json?: Record<string, unknown>;
  reason?: string;
  status: RequestStatus;
  requester_id: number;
  reviewer_id?: number;
  review_comment?: string;
  created_at: string;
  reviewed_at?: string;
}

export interface UserRequestItem {
  id: number;
  action_type: "CREATE" | "DELETE";
  payload_json?: Record<string, unknown>;
  reason?: string;
  status: RequestStatus;
  requester_id: number;
  reviewer_id?: number;
  review_comment?: string;
  created_at: string;
  reviewed_at?: string;
}

export interface ApiError {
  detail: string;
}
