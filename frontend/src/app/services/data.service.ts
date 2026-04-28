import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class DataService {
  constructor(private http: HttpClient) {}

  getPatientData(patientId: string, date: string) {
    if (environment.dataSource === 'local') {
      return this.http.get(`${environment.dataApiUrl}/gold/${patientId}/${date}`);
    } else {
      // Firebase logic here
      return this.http.get(`your-firebase-endpoint`);
    }
  }

  getConditions() {
    if (environment.dataSource === 'local') {
      return this.http.get(`${environment.dataApiUrl}/gold/_conditions.json`);
    } else {
      // Firebase logic here
      return this.http.get(`your-firebase-conditions-endpoint`);
    }
  }

  getH3Reference() {
    if (environment.dataSource === 'local') {
      return this.http.get(`${environment.dataApiUrl}/gold/_h3_reference.json`);
    } else {
      // Firebase logic here
      return this.http.get(`your-firebase-h3-reference-endpoint`);
    }
  }
}
