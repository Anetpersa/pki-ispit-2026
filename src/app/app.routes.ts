import { Routes } from '@angular/router';
import { Home } from './components/home/home';
import { Catalog } from './components/catalog/catalog';
import { Login } from './components/login/login';
import { Signup } from './components/signup/signup';

export const routes: Routes = [
  { path: '', title: 'ToyBox', component: Home },
  { path: 'igracke', title: 'Igračke', component: Catalog },
  { path: 'login', title: 'Prijava', component: Login },
  { path: 'signup', title: 'Registracija', component: Signup },
  { path: '**', redirectTo: '' },
];