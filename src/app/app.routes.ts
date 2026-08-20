import { Routes } from '@angular/router';
import { Home } from './home/home';

export const routes: Routes = [
  { path: '', title: 'Igračke', component: Home },
  { path: '**', redirectTo: '' },
];