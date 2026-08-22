import { Routes } from '@angular/router';
import { Home } from './components/home/home';
import { Catalog } from './components/catalog/catalog';
import { Login } from './components/login/login';
import { Signup } from './components/signup/signup';
import { Profile } from './components/profile/profile';
import { Cart } from './components/cart/cart';
import { Details } from './components/details/details';
import { authGuard } from '../guards/auth.guard';

export const routes: Routes = [
  { path: '', title: 'ToyBox', component: Home },
  { path: 'igracke', title: 'Igračke', component: Catalog },
  { path: 'igracke/:permalink', title: 'Detalji igračke', component: Details },
  { path: 'login', title: 'Prijava', component: Login },
  { path: 'signup', title: 'Registracija', component: Signup },
  { path: 'profil', title: 'Profil', component: Profile, canActivate: [authGuard] },
  { path: 'cart', title: 'Korpa', component: Cart, canActivate: [authGuard] },
  { path: '**', redirectTo: '' },
];