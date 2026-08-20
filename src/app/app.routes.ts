import { Routes } from '@angular/router';
import { Home } from './home/home';
import { Catalog } from './catalog/catalog';

export const routes: Routes = [
  { path: '', title: 'ToyBox', component: Home },
  { path: 'igracke', title: 'Igračke', component: Catalog },
  { path: '**', redirectTo: '' },
];