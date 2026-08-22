export type ReviewAuthorType = 'dete' | 'roditelj';

export interface ReviewModel {
  reviewId: string;
  toyId: number;
  authorName: string;
  authorType: ReviewAuthorType;
  rating: number;
  comment: string;
  date: string;
}